%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc DBSync hooks.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_events).
-author("Rafal Slota").

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([change_replicated/2, schedule_file_location_handling/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Hook that runs just after change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated(SpaceId :: binary(), dbsync_worker:change()) ->
    any().
change_replicated(SpaceId, #change{model = file_meta, doc = FileDoc = #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}}}) ->
    ?info("change_replicated: changed file_meta reg file ~p", [FileUUID]),
    fslogic_file_location:create_storage_file_if_not_exists(SpaceId, FileDoc),
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []);
change_replicated(_SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = #file_meta{}}}) ->
    ?info("change_replicated: changed file_meta ~p", [FileUUID]),
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []);
change_replicated(SpaceId, #change{model = file_location, doc = Doc = #document{value = #file_location{uuid = FileUUID}}}) ->
    ?info("change_replicated: changed file_location ~p", [FileUUID]),
    case replication_dbsync_hook:on_file_location_change(SpaceId, Doc) of
        ok ->
            ok;
        {error,{{badmatch,{error,{not_found,file_meta}}}, _}} ->
            spawn(?MODULE, schedule_file_location_handling, [SpaceId, Doc, 0]);
        Error ->
            ?error("on_file_location_change error: ~p", [Error])
    end;
change_replicated(_SpaceId, _Change) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_file_location_handling(SpaceId, Doc, N) when N < 15 -> %todo use definition cache_to_disk_force_delay_ms
    timer:sleep(timer:seconds(1)),
    case replication_dbsync_hook:on_file_location_change(SpaceId, Doc) of
        ok ->
            ok;
        {error,{{badmatch,{error,{not_found,file_meta}}}, _}} ->
            schedule_file_location_handling(SpaceId, Doc, N + 1);
        Error ->
            ?error("on_file_location_change error: ~p", [Error])
    end;
schedule_file_location_handling(_SpaceId, Doc, N) ->
    ?warning("Could not apply file location changes: ~p, after ~p retries, due to missing file_meta", [Doc, N]).
