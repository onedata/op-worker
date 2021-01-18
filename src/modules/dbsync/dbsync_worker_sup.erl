%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions operating on dbsync worker supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_worker_sup).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([supervisor_flags/0]).
-export([start_in_stream/1, stop_in_stream/1]).
-export([start_out_stream/1, stop_out_stream/1]).
-export([start_on_demand_stream/3, get_on_demand_changes_stream_id/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1000, period => 3600}.


-spec start_in_stream(od_space:id()) -> ok.
start_in_stream(SpaceId) ->
    Spec = dbsync_in_stream_spec(SpaceId),
    {ok, _} = supervisor:start_child(?DBSYNC_WORKER_SUP, Spec),
    ok.

-spec stop_in_stream(od_space:id()) -> ok | no_return().
stop_in_stream(SpaceId) ->
    ok = supervisor:terminate_child(?DBSYNC_WORKER_SUP, ?IN_STREAM_ID(SpaceId)),
    ok = supervisor:delete_child(?DBSYNC_WORKER_SUP, ?IN_STREAM_ID(SpaceId)).


-spec start_out_stream(od_space:id()) -> ok.
start_out_stream(SpaceId) ->
    Opts = dbsync_worker:get_main_out_stream_opts(SpaceId),
    Spec = dbsync_out_stream_spec(SpaceId, SpaceId, Opts),
    {ok, _} = supervisor:start_child(?DBSYNC_WORKER_SUP, Spec),
    ok.

-spec stop_out_stream(od_space:id()) -> ok | no_return().
stop_out_stream(SpaceId) ->
    ok = supervisor:terminate_child(?DBSYNC_WORKER_SUP, ?OUT_STREAM_ID(SpaceId)),
    ok = supervisor:delete_child(?DBSYNC_WORKER_SUP, ?OUT_STREAM_ID(SpaceId)).


-spec start_on_demand_stream(od_space:id(), od_provider:id(), [dbsync_out_stream:option()]) -> ok.
start_on_demand_stream(SpaceId, ProviderId, Opts) ->
    Name = get_on_demand_changes_stream_id(SpaceId, ProviderId),
    StreamId = ?OUT_STREAM_ID(Name),
    critical_section:run([?MODULE, StreamId], fun() ->
        case global:whereis_name(StreamId) of
            undefined ->
                Node = datastore_key:any_responsible_node(SpaceId),
                % TODO VFS-VFS-6651 - child deletion will not be needed after
                % refactoring of supervision tree to use one_for_one supervisor
                rpc:call(Node, supervisor, terminate_child, [?DBSYNC_WORKER_SUP, StreamId]),
                rpc:call(Node, supervisor, delete_child, [?DBSYNC_WORKER_SUP, StreamId]),

                Spec = dbsync_out_stream_spec(Name, SpaceId, Opts),
                try
                    {ok, _} = rpc:call(Node, supervisor, start_child, [?DBSYNC_WORKER_SUP, Spec]),
                    ok
                catch
                    Error:Reason ->
                        ?error("Error when starting stream on demand ~p:~p", [Error, Reason])
                end;
            _ ->
                ok
        end
    end).

-spec get_on_demand_changes_stream_id(od_space:id(), od_provider:id()) -> binary().
get_on_demand_changes_stream_id(SpaceId, ProviderId) ->
    <<SpaceId/binary, "_", ProviderId/binary>>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec dbsync_in_stream_spec(od_space:id()) -> supervisor:child_spec().
dbsync_in_stream_spec(SpaceId) ->
    #{
        id => ?IN_STREAM_ID(SpaceId),
        start => {dbsync_in_stream, start_link, [SpaceId]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [dbsync_in_stream]
    }.

%% @private
-spec dbsync_out_stream_spec(binary(), od_space:id(),
    [dbsync_out_stream:option()]) -> supervisor:child_spec().
dbsync_out_stream_spec(ReqId, SpaceId, Opts) ->
    #{
        id => ?OUT_STREAM_ID(ReqId),
        start => {dbsync_out_stream, start_link, [ReqId, SpaceId, Opts]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [dbsync_out_stream]
    }.
