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
-export([change_replicated/2]).

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
change_replicated(SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE, mode = Mode}}}) ->
    ?debug("change_replicated: changed file_meta ~p", [FileUUID]),
    fslogic_file_location:create_storage_file_if_not_exists(SpaceId, FileUUID, ?ROOT_SESS_ID, Mode), %todo create with appropriate owner
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []);
change_replicated(_SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = #file_meta{}}}) ->
    ?debug("change_replicated: changed file_meta ~p", [FileUUID]),
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []);
change_replicated(SpaceId, #change{model = file_location, doc = Doc = #document{value = #file_location{uuid = FileUUID}}}) ->
    ?debug("change_replicated: changed file_location ~p", [FileUUID]),
    replication_dbsync_hook:on_file_location_change(SpaceId, Doc);
%%    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []),
%%    fslogic_event:emit_file_location_update({uuid, FileUUID}, []);
change_replicated(_SpaceId, _Change) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================