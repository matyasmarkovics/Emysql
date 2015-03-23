%% Copyright (c) 2009-2012
%% Bill Warnecke <bill@rupture.com>,
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>,
%% Henning Diedrich <hd2010@eonblast.com>,
%% Eonblast Corporation <http://www.eonblast.com>
%% Matyas Markovics <markovics.matyas@gmail.com>
%% 
%% Permission is  hereby  granted,  free of charge,  to any person
%% obtaining  a copy of this software and associated documentation
%% files (the "Software"),to deal in the Software without restric-
%% tion,  including  without  limitation the rights to use,  copy, 
%% modify, merge,  publish,  distribute,  sublicense,  and/or sell
%% copies  of the  Software,  and to  permit  persons to  whom the
%% Software  is  furnished  to do  so,  subject  to the  following 
%% conditions:
%% 
%% The above  copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF  MERCHANTABILITY,  FITNESS  FOR  A  PARTICULAR  PURPOSE  AND
%% NONINFRINGEMENT. IN  NO  EVENT  SHALL  THE AUTHORS OR COPYRIGHT
%% HOLDERS  BE  LIABLE FOR  ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT,  TORT  OR OTHERWISE,  ARISING
%% FROM,  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.

-module(emysequel_conn_srv).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([
    start_link/1, 
    fetch/2, fetch/3,
    execute/2, execute/3, execute/4
    ]).

-export([
    init/1,
    terminate/2
    ]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
    ]).


-include("emysql.hrl").

-record(state, {
        init_time,
        host,
        port,
        socket,
        user,
        password,
        greeting,
        collation,
        database,
        prepares,
        args = dict:new(),
        caller = none,
        results = []
    }).


%%--------------------------------------------------------------------
%%% Examples
%%--------------------------------------------------------------------
%%
%% {ok, Pid} =                                                                                                                               
%% emysequel_conn:start_link([{host, "localhost"},{port, 3306}, {user, <<"root">>}, {password, <<"password">>}, {collation, <<"utf8">>}, {database, <<"test">>}], 1000).
%% 
%% Received: {tcp,#Port<0.702>,
%%                <<78,0,0,0,10,53,46,53,46,51,48,45,108,111,103,0,1,0,0,0,84,39,
%%                  99,61,72,48,87,68,0,255,247,33,2,0,15,128,21,0,0,0,0,0,0,0,0,
%%                  0,0,49,100,40,57,38,80,67,63,81,88,71,58,0,109,121,115,113,
%%                  108,95,110,97,116,105,118,101,95,112,97,115,115,119,111,114,
%%                  100,0>>}
%% Received: {tcp_closed,#Port<0.702>}
%% 
%% emysequel_conn:fetch(Pid, <<"SHOW TABLES">>).
%% 
%% 
%%
%%
%%

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
start_link(Config) ->
    Timeout = proplists:get_value(init_time, Config, ?APP:default_timeout()),
    gen_server:start_link(?MODULE, [ {init_time, Timeout} |Config], [{timeout, Timeout}]).


fetch(Self, Query) ->
    fetch(Self, Query, []).

fetch(Self, Query, Options) ->
    call(Self, {{fetch, Query}, Options}).


execute(Self, Name) ->
    execute(Self, Name, []).

execute(Self, Name, Args) ->
    execute(Self, Name, Args, []).

execute(Self, Name, Args, Options) when is_list(Args) ->
    Used = lists:sum([
                begin
                    Start = erlang:now(),
                    gen_server:call(Self, {set, Arg}),
                    timer:now_diff(Start, erlang:now()) div 1000
                end || Arg <- Args ]),
    Timeout = proplists:get_value(timeout, Options, ?APP:default_timeout()),
    NewOptions = lists:keyreplace(timeout, 1, Options, {timeout, Timeout-Used}),
    call(Self, {{execute, Name}, NewOptions}).

%% Not allowing these
%prepare(Self, Name, Statement) ->
%    gen_fsm:sync_send_event(Self, {prepare, Name, Statement}).
%
%prepare(Self, Name, Statement, Timeout) ->
%    gen_fsm:sync_send_event(Self, {prepare, Name, Statement}, Timeout).
%
%
%command(Self, Command) ->
%    gen_fsm:sync_send_event(Self, {cmd, Command}).


call(Self, {Call, Options}) ->
    Timeout = proplists:get_value(timeout, Options, ?APP:default_timeout()),
    Mode = proplists:get_value(active, Options, true),
    call(Self, {Call, Options}, Timeout, Mode).


call(Self, CallOpts, Timeout, false) ->
    Start = erlang:now(),
    {ok, Socket} = gen_server:call(Self, CallOpts, Timeout),
    Used = timer:now_diff(Start, erlang:now()) div 1000,
    {Result, _} = emysql_tcp:recv_parse(Socket, Timeout-Used),
    gen_server:cast(Self, {socket, Socket}),
    Result;
call(Self, Event, Timeout, true) ->
    gen_server:call(Self, Event, Timeout).



%%-------------------------------------------------------------------
%%% Behaviour Callbacks
%%--------------------------------------------------------------------
init(Config) ->
    case prepare(authenticate(connect(config_to_state(validate_config(Config))))) of
        #state{} = S ->
            {ok, S};
        {error, Reason} ->
            {stop, Reason}
    end.


connect({error, Reason}) -> {error, Reason};
connect(S = #state{ host = Host, port = Port }) ->
    Timeout = S#state.init_time, 
    Start = erlang:now(),
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, once}, {keepalive, true},
                                      {buffer, ?MAXPACKETSIZE}, %% MySQL Max Packet Size, 16MB
                                      {send_timeout, ?APP:default_timeout()},
                                      {send_timeout_close, true}]) of
        {error, Reason} -> {error, {failed_to_connect, Reason}};
        {ok, Socket} ->
            gen_tcp:controlling_process(Socket, self()),
            Used = timer:now_diff(Start, erlang:now()) div 1000,
            S#state{ socket = Socket, init_time = Timeout-Used }
    end.


authenticate({error, Reason}) -> {error, Reason};
authenticate(S = #state{ socket = Socket, user = User, password = Password, collation = Collation, database = Database }) ->
    Timeout = S#state.init_time,
    {Packet, Used} = emysql_tcp:recv_packet(Socket, Timeout),
    {GreetingPacket, <<>>} = emysql_tcp:packets(Packet),
    Greeting = #greeting{ salt1 = Salt1, salt2 = Salt2, plugin = Plugin } = emysql_auth:read_greeting(GreetingPacket),
    AuthPacket = emysql_auth:auth_packet(User, Password, Salt1, Salt2, Plugin, Collation, Database),
    case emysql_tcp:send_and_recv_packet(Socket, Timeout-Used, AuthPacket, GreetingPacket#packet.seq_num+1) of
        {#ok_packet{}, RemTime} ->  S#state{ greeting = Greeting, init_time = RemTime };
        {#error_packet{ msg = Msg }, _} -> {error, {access_denied, Msg}}
    end.

prepare({error, Reason}) -> {error, Reason};
prepare(S = #state{ prepares = [] }) -> S;
prepare(S = #state{ socket = Socket, args = ArgsDict, prepares = [{Name, Statement} |Prepares] }) ->
    Timeout = S#state.init_time,
    PreparedStatement = replace_args(Statement),
    Packet = <<?COM_QUERY, "PREPARE `", Name/binary, "` FROM '", PreparedStatement/binary, "'">>,
    Next = case emysql_tcp:send_and_recv_packet(Socket, Timeout, Packet, 0) of
        {#ok_packet{}, RemTime} -> 
            S#state{ args = dict:store(Name, parse_args(Statement), ArgsDict), prepares = Prepares, init_time = RemTime };
        {#error_packet{ msg = Msg }, _} ->
            {error, {failed_to_prepare, Msg}}
    end,
    prepare(Next).

handle_call(_, From, S = #state{ caller = Caller }) when Caller /= none andalso From /= Caller ->
    {reply, busy, S};

handle_call({{fetch, Query}, Opts}, From, S = #state{ socket = Socket }) when is_binary(Query) ->
    Packet = <<?COM_QUERY, Query/binary>>,
    emysql_tcp:send_packet(Socket, Packet, 0),
    mode_switch(Socket, From, Opts),
    {noreply, S#state{ caller = From }};

handle_call({set, {Key, Value}}, From, S = #state{ socket = Socket }) ->
    Packet = <<?COM_QUERY, "SET @", Key/binary, "=", Value/binary>>,
    emysql_tcp:send_packet(Socket, Packet, 0),
    inet:setopts(Socket, [{active, once}]),
    {noreply, S#state{ caller = From }};

handle_call({{execute, Name}, Opts}, From, S = #state{ socket = Socket, args = ArgsDict }) when is_binary(Name) ->
    case dict:find(Name, ArgsDict) of
        error ->
            {reply, {error, not_prepared}, S};
        {ok, Args} ->
            BinArgs = bin_join(Args, <<", ">>, <<>>),
            Packet = bin_join([<<?COM_QUERY, "EXECUTE ", Name/binary>>, BinArgs], <<" USING ">>, <<>>),
            emysql_tcp:send_packet(Socket, Packet, 0),
            mode_switch(Socket, From, Opts),
            {noreply, S#state{ caller = From }}
    end;

handle_call({prepare, Name, Statement}, _, S = #state{ prepares = Prepares }) ->
    case prepare(S#state{ prepares = [{Name, Statement}|Prepares] }) of
        #state{} = NewState -> {reply, ok, NewState};
        {error, _} = Error -> {reply, Error, S}
    end;

% below is a whole through to try out commands other then ?COM_QUERY
handle_call({cmd, Command}, From, S = #state{ socket = Socket }) when is_binary(Command) ->
    emysql_tcp:send_packet(Socket, Command, 0),
    inet:setopts(Socket, [{active, once}]),
    {noreply, S#state{ caller = From }}.


handle_cast({socket, Socket}, S) -> 
    erlang:port_connect(Socket, self()),
    {noreply, S#state{ socket = Socket }}.


handle_packet(#ok_packet{} = Ok, S) ->
    {noreply, reply(Ok, S)};
handle_packet(#error_packet{} = Err, S) ->
    {noreply, reply(Err, S)};
handle_packet(#result_packet{ extra = #eof_packet{ status = ServerStatus } } = Res, S = #state{ results = Results }) when ServerStatus band ?SERVER_MORE_RESULTS_EXIST ->
    {noreply, S#state{ results = [Res|Results] }};
handle_packet(#result_packet{} = Res, S = #state{ results = [] }) ->
    {noreply, reply(Res, S)};
handle_packet(#result_packet{} = Res, S = #state{ results = Results }) ->
    {noreply, reply(lists:reverse([Res|Results]), S#state{ results = [] })}.


handle_info({tcp, Socket, Packet}, S = #state{ socket = Socket }) ->
    handle_packet(emysql_tcp:parse(Packet), S);

handle_info({tcp_closed, Socket}, S = #state{ socket = Socket }) ->
    {stop, tcp_closed, S};

handle_info({Socket, connected}, S = #state{ socket = Socket }) ->
    {noreply, reply({ok, Socket}, S#state{ socket = undefined })}; 

handle_info(Info, State) -> 
    io:format("Unexpected msg: ~p~n", [Info]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_, #state{ socket = undefined }) -> ok;
terminate(_, #state{ socket = Socket }) ->
    gen_tcp:close(Socket).



%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% Mode switch of port handling, the port is either to receive a msg or to be given away for reading of results.
mode_switch(Socket, {FromPid, _FromRef}, Options) ->
    case proplists:get_value(active, Options, true) of
        % Active mode, received data massage passed
        true -> inet:setopts(Socket, [{active, once}]);
        % Passive mode, data goes directly to caller
        false -> Socket ! {self(), {connect, FromPid}}
    end.

reply(Reply, S = #state{ caller = Caller }) ->
    gen_server:reply(Caller, Reply),
    S#state{ caller = none }.

replace_args(Statement) ->
    re:replace(Statement, "\\@\\w+", "\\?", [global, {return, binary}]).

parse_args(Statement) ->
    case re:run(Statement, "\\@\\w+", [global, {capture, all, binary}]) of
        {match, Matched} ->
            [Key || [<<Key/binary>>|_] <- Matched];
        nomatch -> []
    end.


%%--------------------------------------------------------------------
%%% Utility functions
%%--------------------------------------------------------------------
validate_config(Config) ->
    validate_config(Config, Config).

validate_config(C, []) -> C;
validate_config(C, [{host, Value}|T]) when is_list(Value) ->
    validate_config(C, T);
validate_config(C, [{port, Value}|T]) when is_integer(Value) ->
    validate_config(C, T);
validate_config(C, [{init_time, Value}|T]) when is_integer(Value) ->
    validate_config(C, T);
validate_config(C, [{collation, Value}|T]) when is_binary(Value) ->
    case proplists:is_defined(Value, ?COLLATIONS) of
        true -> validate_config(C, T);
        false -> {error, {invalid_config, {collation, Value}}}
    end;
validate_config(C, [{Key, Value}|T]) when is_atom(Key) andalso is_binary(Value) ->
    validate_config(C, T);
validate_config(_, Invaild) ->
    {error, {invalid_config, Invaild}}.


config_to_state({error, _Reason} = Error) -> Error;
config_to_state(Proplist) when is_list(Proplist) ->
    State = lists:zipwith(
            fun (undefined, Field) -> proplists:get_value(Field, Proplist);
                (Defined, _) -> Defined
            end,
            tl(tuple_to_list(#state{})),
            record_info(fields, state)
            ),
    list_to_tuple([ state |State]).


bin_join([], _, Acc) -> Acc;
bin_join([H], _, Acc) -> <<Acc/binary, H/binary>>;
%skip empty binaries in the join list
bin_join([H|T], Sep, Acc) when hd(T) == <<>> ->
    bin_join([H|tl(T)], Sep, Acc);
bin_join([H|T], Sep, Acc) ->
    bin_join(T, Sep, <<Acc/binary, H/binary, Sep/binary>>).

