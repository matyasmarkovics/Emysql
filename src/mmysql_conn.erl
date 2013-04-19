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

-module(mmysql_conn).
-behaviour(gen_fsm).

-export([
    start_link/1, start_link/2,
    fetch/2, fetch/3,
    execute/2, execute/3, execute/4
    ]).

-export([
    init/1,
    connected/2,
    pending_prepares/2,
    ready/2, ready/3, ready_blocked/2,
    prepared/2, prepared/3, prepared_blocked/2,
    terminate/3
    ]).

-export([
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    code_change/4
    ]).

-export([hstate/1]).

-include("emysql.hrl").

-record(state, {
        host,
        port,
        socket,
        user,
        password,
        greeting,
        collation,
        database,
        prepares,
        staging = none,
        args = dict:new(),
        callers = dict:new()
    }).


%%--------------------------------------------------------------------
%%% Examples
%%--------------------------------------------------------------------
%%
%% {ok, Pid} =                                                                                                                               
%% mmysql_conn:start_link([{host, "localhost"},{port, 3306}, {user, <<"root">>}, {password, <<"password">>}, {collation, <<"utf8">>}, {database, <<"test">>}], 1000).
%% 
%% Received: {tcp,#Port<0.702>,
%%                <<78,0,0,0,10,53,46,53,46,51,48,45,108,111,103,0,1,0,0,0,84,39,
%%                  99,61,72,48,87,68,0,255,247,33,2,0,15,128,21,0,0,0,0,0,0,0,0,
%%                  0,0,49,100,40,57,38,80,67,63,81,88,71,58,0,109,121,115,113,
%%                  108,95,110,97,116,105,118,101,95,112,97,115,115,119,111,114,
%%                  100,0>>}
%% Received: {tcp_closed,#Port<0.702>}
%% 
%% mmysql_conn:fetch(Pid, <<"SHOW TABLES">>).
%% 
%% 
%%
%%
%%

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
start_link(Config) ->
    start_link(Config, ?TIMEOUT).

start_link(Config, Timeout) ->
    gen_fsm:start_link(?MODULE, Config, [{timeout, Timeout}]).


fetch(Self, Query) ->
    fetch(Self, Query, []).

fetch(Self, Query, Options) ->
    Timeout = proplists:get_value(timeout, Options, ?TIMEOUT),
    case proplists:get_value(active, Options, true) of
        false = M ->
            passive_sync_send_event(Self, {fetch, M, Query}, Timeout);
        true = M ->
            gen_fsm:sync_send_event(Self, {fetch, M, Query}, Timeout)
    end.

execute(Self, Name) ->
    execute(Self, Name, []).

execute(Self, Name, Args) ->
    execute(Self, Name, Args, []).

execute(Self, Name, Args, Options) when is_list(Args) ->
    Used = lists:sum([
                begin
                    Start = erlang:now(),
                    gen_fsm:sync_send_event(Self, {set, Arg}),
                    timer:now_diff(Start, erlang:now()) div 1000
                end || Arg <- Args ]),
    Timeout = proplists:get_value(timeout, Options, ?TIMEOUT),
    case proplists:get_value(active, Options, true) of
        false = M ->
            passive_sync_send_event(Self, {execute, M, Name}, Timeout-Used);
        true = M ->
            gen_fsm:sync_send_event(Self, {execute, M, Name}, Timeout-Used)
    end.

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


passive_sync_send_event(Self, Event, Timeout) ->
    Start = erlang:now(),
    {ok, Socket} = gen_fsm:sync_send_event(Self, Event, Timeout),
    Used = timer:now_diff(Start, erlang:now()) div 1000,
    %gen_tcp:controlling_process(Socket, self()),
    erlang:port_info(Socket),
    Result = case emysql_tcp:response_list(Socket, Timeout-Used, ?SERVER_MORE_RESULTS_EXIST, []) of
        [Record] -> Record;
        List when is_list(List) -> List
    end,
    gen_fsm:send_event(Self, {socket, Socket}),
    Result.


%%-------------------------------------------------------------------
%%% Behaviour Callbacks
%%--------------------------------------------------------------------
init(Config) ->
    case connect(config_to_state(validate_config(Config))) of
        {ok, Socket, S} ->
            gen_tcp:controlling_process(Socket, self()),
            {ok, connected, S#state{ socket = Socket }};
        {error, Reason, S} ->
            {stop, Reason, S};
        {error, Reason} ->
            {stop, {invalid_configuration, Reason}, #state{}}
    end.

connect({error, Reason}) -> {error, Reason};
connect(S = #state{ host = Host, port = Port }) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, once}, {keepalive, true},
                                      {buffer, ?MAXPACKETSIZE}, %% MySQL Max Packet Size, 16MB
                                      {send_timeout, ?TIMEOUT}, {send_timeout_close, true}]) of
        {ok, Socket} -> {ok, Socket, S};
        {error, Reason} -> {error, Reason, S}
    end.


connected(_, S) -> {next_state, connected, S}.


pending_prepares(timeout, S = #state{ prepares = undefined }) ->
    {next_state, ready, S};

pending_prepares(timeout, S = #state{ prepares = [] }) ->
    {next_state, prepared, S};

pending_prepares(timeout, S = #state{ socket = Socket, prepares = [{Name, Statement} |Prepares] }) ->
    PreparedStatement = replace_args(Statement),
    Packet = <<?COM_QUERY, "PREPARE `", Name/binary, "` FROM '", PreparedStatement/binary, "'">>,
    emysql_tcp:send_packet(Socket, Packet, 0),
    inet:setopts(Socket, [{active, once}]),
    {next_state, receiving_prepares, S#state{ prepares = Prepares, staging = {Name, Statement} }}.


ready(_, S) -> {next_state, ready, S}.

%% in order to work with poolboy, who doesn't know about states,
%% preparing statements run-time is restricted.
%ready({prepare, Name, Statement}, From, S = #state{ callers = Callers }) ->
%    NewSate = S#state{
%            prepares = [{Name, Statement}],
%            callers = dict:append(prepare, From, Callers)
%            },
%    {next_state, pending_prepares, NewSate, 1};

%% below is a whole through to try out commands other then ?COM_QUERY
%ready({cmd, Command}, From, S = #state{ socket = Socket, callers = Callers }) when is_binary(Command) ->
%    emysql_tcp:send_packet(Socket, Command, 0),
%    inet:setopts(Socket, [{active, once}]),
%    {next_state, receiving_fetch, S#state{ callers = dict:append(fetch, From, Callers) }};

ready({fetch, Mode, Query}, From, S = #state{ socket = Socket, callers = Callers }) when is_binary(Query) ->
    Packet = <<?COM_QUERY, Query/binary>>,
    emysql_tcp:send_packet(Socket, Packet, 0),
    mode_switch(Socket, From, Mode),
    {next_state, receiving_fetch, S#state{ callers = dict:append(fetch, From, Callers) }}.


ready_blocked({socket, Socket}, S) -> 
    erlang:port_connect(Socket, self()),
    {next_state, ready, S#state{ socket = Socket }}.

prepared(_, S) -> {next_state, prepared, S}.

%prepared({prepare, _, _} = Cmd, From, S) -> ready(Cmd, From, S);
prepared({fetch, _} = Cmd, From, S) -> ready(Cmd, From, S);
%prepared({cmd, _} = Cmd, From, S) -> ready(Cmd, From, S);

prepared({set, {Key, Value}}, From, S = #state{ socket = Socket, callers = Callers }) ->
    Packet = <<?COM_QUERY, "SET @", Key/binary, "=", Value/binary>>,
    emysql_tcp:send_packet(Socket, Packet, 0),
    inet:setopts(Socket, [{active, once}]),
    {next_state, receiving_set, S#state{ callers = dict:append(set, From, Callers) }};

prepared({execute, Mode, Name}, From, S = #state{ socket = Socket, args = ArgsDict, callers = Callers }) when is_binary(Name) ->
    SEND = fun(Packet) ->
            emysql_tcp:send_packet(Socket, Packet, 0),
            mode_switch(Socket, From, Mode)
    end,
    case dict:find(Name, ArgsDict) of
        error ->
            {reply, {error, not_prepared}, prepared, S};
        {ok, []} ->
            SEND(<<?COM_QUERY, "EXECUTE ", Name/binary>>),
            {next_state, receiving_execute, S#state{ callers = dict:append(execute, From, Callers) }};
        {ok, Args} ->
            SEND(bin_join(Args, <<", ">>, <<?COM_QUERY, "EXECUTE ", Name/binary, " USING ">>)),
            {next_state, receiving_execute, S#state{ callers = dict:append(execute, From, Callers) }}
    end. 

prepared_blocked({socket, Socket}, S) ->
    erlang:port_connect(Socket, self()),
    {next_state, prepared, S#state{ socket = Socket }}.

terminate(_, _, #state{ socket = undefined }) -> ok;
terminate(_, _, #state{ socket = Socket }) ->
    gen_tcp:close(Socket).


%%--------------------------------------------------------------------
%%% Undefined Callbacks
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.


handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.


handle_packet(#ok_packet{}, receiving_authentication, S) ->
    {next_state, pending_prepares, S, 1};
handle_packet(#error_packet{ msg = Msg }, receiving_authentication, S) ->
    {stop, {access_denied, Msg}, S};

handle_packet(#ok_packet{} = Ok, receiving_prepares, S = #state{ staging = {Name, Statement}, args = ArgsDict }) ->
    NewState = S#state{ args = dict:store(Name, parse_args(Statement), ArgsDict), staging = none },
    {next_state, pending_prepares, reply(prepare, Ok, NewState), 1};
handle_packet(#error_packet{ msg = Msg } = Err, receiving_prepares, S) ->
    {stop, {failed_to_prepare_statement, Msg}, reply(prepare, Err, S)};

handle_packet(#ok_packet{} = Ok, receiving_set, S) ->
    {next_state, prepared, reply(set, Ok, S)};
handle_packet(#error_packet{} = Err, receiving_set, S) ->
    {next_state, prepared, reply(set, Err, S)};

handle_packet(#ok_packet{} = Ok, receiving_fetch, S = #state{ args = ArgsDict }) ->
    {next_state, case dict:size(ArgsDict) of 0 -> ready; _ -> prepared end, reply(fetch, Ok, S)};
handle_packet(#error_packet{} = Err, receiving_fetch, S = #state{ args = ArgsDict }) ->
    {next_state, case dict:size(ArgsDict) of 0 -> ready; _ -> prepared end, reply(fetch, Err, S)};
handle_packet(#result_packet{} = Result, receiving_fetch, S = #state{ args = ArgsDict }) ->
    {next_state, case dict:size(ArgsDict) of 0 -> ready; _ -> prepared end, reply(fetch, Result, S)};
    
handle_packet(#ok_packet{} = Ok, receiving_execute, S) ->
    {next_state, prepared, reply(execute, Ok, S)};
handle_packet(#error_packet{} = Err, receiving_execute, S) ->
    {next_state, prepared, reply(execute, Err, S)};
handle_packet(#result_packet{} = Result, receiving_execute, S) ->
    {next_state, prepared, reply(execute, Result, S)}.


handle_info({tcp, Socket, Packet}, connected, S = #state{ socket = Socket, user = User, password = Password, collation = Collation, database = Database }) ->
    {GreetingPacket, <<>>} = emysql_tcp:packets(Packet),
    Greeting = #greeting{ salt1 = Salt1, salt2 = Salt2, plugin = Plugin } = emysql_auth:read_greeting(GreetingPacket),
    AuthPacket = emysql_auth:auth_packet(User, Password, Salt1, Salt2, Plugin, Collation, Database),
    emysql_tcp:send_packet(Socket, AuthPacket, GreetingPacket#packet.seq_num+1),
    inet:setopts(Socket, [{active, once}]),
    {next_state, receiving_authentication, S#state{ greeting = Greeting }, 1};

handle_info({tcp, Socket, Packet}, StateName, S = #state{ socket = Socket }) ->
    handle_packet(emysql_tcp:parse(Packet), StateName, S);

handle_info({tcp_closed, Socket}, _, S = #state{ socket = Socket }) ->
    {stop, tcp_closed, S};

handle_info({Socket, connected}, receiving_fetch, S = #state{ socket = Socket }) ->
    {next_state, ready_blocked, reply(fetch, {ok, Socket}, S#state{ socket = undefined })}; 

handle_info(Info, StateName, State) -> 
    io:format("In state: ~p, Received: ~p~n", [StateName, Info]),
    {next_state, StateName, State}.


code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}
        .

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% Mode switch of port handling, the port is either to receive a msg or to be given away for reading of results.
% Active mode, received data massage passed
mode_switch(Socket, _, true) ->
    inet:setopts(Socket, [{active, once}]);
% Passive mode, data goes directly to caller
mode_switch(Socket, From, false) ->
    io:format("socket info: ~p~n", [erlang:port_info(Socket)]),
    {FromPid, _FromRef} = From,
    Socket ! {self(), {connect, FromPid}}.

reply(Name, Reply = #result_packet{ extra = #eof_packet{ status = ServerStatus } }, S = #state{ callers = Callers })
        when ServerStatus band ?SERVER_MORE_RESULTS_EXIST ->
    case dict:is_key(Name, Callers) of
        true ->
            S#state{ callers = dict:append(Name, Reply, Callers) };
        _ -> S
    end;
reply(Name, Reply, S = #state{ callers = Callers }) ->
    case dict:find(Name, Callers) of
        {ok, [From]} ->
            gen_fsm:reply(From, Reply),
            S#state{ callers = dict:erase(Name, Callers) };
        {ok, [From |Results]} ->
            gen_fsm:reply(From, Results ++ [Reply]),
            S#state{ callers = dict:erase(Name, Callers) };
        _ -> S
    end.

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
validate_config(C, [{host, _}|T]) ->
    validate_config(C, T);
validate_config(C, [{port, Value}|T]) when is_integer(Value) ->
    validate_config(C, T);
validate_config(C, [{collation, Value}|T]) when is_binary(Value) ->
    case proplists:is_defined(Value) of
        true -> validate_config(C, T);
        false -> {error, {collation, Value}}
    end;
validate_config(C, [{Key, Value}|T]) when is_atom(Key) andalso is_binary(Value) ->
    validate_config(C, T);
validate_config(_, Invaild) ->
    {error, Invaild}.


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


bin_join([H], _, Acc) -> <<Acc/binary, H/binary>>;
bin_join([H|T], Sep, Acc) ->
    bin_join(T, Sep, <<Acc/binary, H/binary, Sep/binary>>).



% human readable string rep of the server state flag
%% @private
hstate(State) ->
    case (State band ?SERVER_STATUS_AUTOCOMMIT) of 0 -> ""; _-> "AUTOCOMMIT " end
        ++ case (State band ?SERVER_MORE_RESULTS_EXIST) of 0 -> ""; _-> "MORE_RESULTS_EXIST " end
            ++ case (State band ?SERVER_QUERY_NO_INDEX_USED) of 0 -> ""; _-> "NO_INDEX_USED " end.
