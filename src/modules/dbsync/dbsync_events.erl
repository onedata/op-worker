%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_events).
-author("Rafal Slota").

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([change_replicated/2]).

%%%===================================================================
%%% API
%%%===================================================================

change_replicated(_SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = #file_meta{}}}) ->
    ?info("NOTIFY changed file ~p", [FileUUID]),
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []);
change_replicated(SpaceId, #change{model = file_location, doc = Doc = #document{value = #file_location{uuid = FileUUID}}}) ->
    replication_dbsync_hook:on_file_location_change(SpaceId, Doc),
    fslogic_event:emit_file_attr_update({uuid, FileUUID}, []),
    fslogic_event:emit_file_location_update({uuid, FileUUID}, []);
change_replicated(_SpaceId, _Change) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================