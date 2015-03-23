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
-export([pool_start/2, pool_start/3, pool_send_event/2]).
%% API entypoint
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

