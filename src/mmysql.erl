-module(mmysql).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0,
         start/2, stop/1]).

-export([init/1]).

-export([call/3]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    supervisor:start_link({local, mmysql_sup}, ?MODULE, []).

stop(_State) ->
    ok.

init([]) ->
    {ok, Pools} = application:get_env(mmysql, pools),
    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}},
                    {worker_module, mmysql_conn}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

call(PoolName, Cmd, Args) ->
    poolboy:transaction(PoolName,
                        fun(Worker) ->
                            apply(mmysql_conn, Cmd, [Worker|Args])
                        end).
