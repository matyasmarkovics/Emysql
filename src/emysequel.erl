-module(emysequel).
-behaviour(application).
-behaviour(supervisor).

%% application callbacks
-export([start/0, stop/0,
         start/2, stop/1]).
%% supervisor callbacks
-export([init/1]).
%% emysql_app compatibility functions
-export([modules/0, default_timeout/0, pools/0]).
%% emysql compatibility functions
-export([add_pool/8, remove_pool/1, 
         increment_pool_size/2, decrement_pool_size/2,
         execute/2, execute/3, execute/4, execute/5]).
%% pool management API
-export([pool_start/2, pool_start/3,
         pool_send_event/2]).
%% database cmd API
-export([call/3, immd/3]).

-include("emysql.hrl").


start() ->
    application:start(?APP).

stop() ->
    application:stop(?APP).


start(_Type, _Args) ->
    supervisor:start_link({local, emysequel_sup}, ?MODULE, []).

stop(_State) ->
    ok.



init([]) ->
    {ok, Pools} = application:get_env(?APP, pools),
    PoolSpecs = lists:map(fun config/1, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.


config({Name, WorkerArgs}) ->
    {SizeArgs, StrictlyWorkerArgs} = split_config(WorkerArgs),
    config({Name, SizeArgs, StrictlyWorkerArgs});
config({Name, SizeArgs, WorkerArgs}) ->
    PoolArgs = poolboy_args(Name, SizeArgs),
    poolboy:child_spec(Name, PoolArgs, WorkerArgs).

split_config(WorkerArgs) ->
    lists:partition(
        fun ({size,_}) -> true;
            ({max_overflow,_}) -> true;
            (_) -> false
        end,
        WorkerArgs).

poolboy_args(Name, SizeArgs) ->
    [
        {name, {local, Name}},
        {worker_module, emysequel_conn},
        {size, proplists:get_value(size, SizeArgs, 1)},
        {max_overflow, proplists:get_value(max_overflow, SizeArgs, 0)}
        ].



modules() ->
    {ok, Modules} = application_controller:get_key(emysql, modules),
    Modules.

default_timeout() ->
    env(default_timeout, ?TIMEOUT).

pools() ->
    env(pools, []).


env(Name, Default) ->
    case application:get_env(?APP, Name) of
        undefined -> Default;
        {ok, Value} -> Value
    end.


pool_start(Name, WorkerArgs) ->
    {SizeArgs, StrictlyWorkerArgs} = split_config(WorkerArgs),
    pool_start(Name, SizeArgs, StrictlyWorkerArgs).

pool_start(Name, SizeArgs, WorkerArgs) ->
    PoolArgs = poolboy_args(Name, SizeArgs),
    poolboy:start_link(PoolArgs, WorkerArgs).

pool_send_event(Name, Event) ->
    gen_fsm:sync_send_all_state_event(Name, Event).




call(PoolName, Cmd, Args) ->
    poolboy:transaction(PoolName,
                        fun(Worker) ->
                            apply(emysequel_conn, Cmd, [Worker|Args])
                        end).

immd(PoolName, Cmd, Args) ->
    case poolboy:checkout(PoolName, false) of
        full -> full;
        Worker ->
            try
                apply(emysequel_conn, Cmd, [Worker|Args])
            after
                ok = poolboy:checkin(PoolName, Worker)
            end
    end.

add_pool(PoolId, Size, User, Password, Host, Port, Database, Encoding) ->
    pool_start(PoolId, [
            {size, Size}
            ], [ 
            {user, list_to_binary(User)},
            {password, list_to_binary(Password)},
            {host, Host},
            {port, Port},
            {database, list_to_binary(Database)},
            {collation, proplists:get_value(Encoding, ?CHARACTERSETS)}
            ]).

remove_pool(PoolId) ->
    pool_send_event(PoolId, stop).

increment_pool_size(PoolId, Num) when is_integer(Num) ->
    pool_send_event(PoolId, {incr, Num}).

decrement_pool_size(PoolId, Num) when is_integer(Num) ->
    pool_send_event(PoolId, {decr, Num}).

execute(PoolId, Query) when (is_list(Query) orelse is_binary(Query)) ->
	call(PoolId, fetch, [Query]);

execute(PoolId, StmtName) when is_atom(StmtName) ->
	call(PoolId, execute, [StmtName]).

execute(PoolId, StmtName, Args) when is_atom(StmtName), is_list(Args) ->
	call(PoolId, execute, [StmtName, Args]);

execute(PoolId, StmtName, Timeout) when is_atom(StmtName), is_integer(Timeout) ->
        call(PoolId, execute, [StmtName, [], [timeout, Timeout]]).

execute(PoolId, StmtName, Args, Timeout) when is_atom(StmtName), is_list(Args) andalso is_integer(Timeout) ->
        call(PoolId, execute, [StmtName, Args, [timeout, Timeout]]).

execute(PoolId, StmtName, Args, Timeout, nonblocking) when is_atom(StmtName), is_list(Args) andalso is_integer(Timeout) ->
        case immd(PoolId, execute, [StmtName, Args, [timeout, Timeout]]) of
                full -> unavailable;
                Result -> Result
        end.
