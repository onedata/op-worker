%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for managing QoS status persistent model.
%%% For more details consult `qos_status` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status_model).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, update/3, get/2, delete/2]).

%% datastore_model callbacks
-export([get_record_struct/1, get_record_version/0]).

-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type id() :: datastore_doc:key().
-type record() :: #qos_status{}.
-type dir_type() :: ?QOS_STATUS_TRAVERSE_CHILD_DIR | ?QOS_STATUS_TRAVERSE_START_DIR.

-export_type([diff/0]).

-define(CTX, (qos_status:get_ctx())).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(od_space:id(), traverse:id(), file_meta:uuid(), dir_type()) -> 
    {ok, doc()}.
create(SpaceId, TraverseId, DirUuid, DirType) ->
    Id = generate_status_doc_id(TraverseId, DirUuid),
    datastore_model:create(?CTX, #document{key = Id, scope = SpaceId,
        value = #qos_status{is_start_dir = DirType == ?QOS_STATUS_TRAVERSE_START_DIR}
    }).


-spec update(traverse:id(), file_meta:uuid(), diff()) -> {ok, doc()} | {error, term()}.
update(TraverseId, Uuid, Diff) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:update(?CTX, Id, Diff).


-spec get(traverse:id(), file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(TraverseId, Uuid) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:get(?CTX, Id).


-spec delete(traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
delete(TraverseId, Uuid)->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:delete(?CTX, Id).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {previous_batch_last_filename, binary},
        {current_batch_last_filename, binary},
        {files_list, [string]},
        {child_dirs_count, integer},
        {is_last_batch, boolean},
        {is_start_dir, boolean}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec generate_status_doc_id(traverse:id(), file_meta:uuid()) -> id().
generate_status_doc_id(TraverseId, DirUuid) ->
    datastore_key:adjacent_from_digest([DirUuid, TraverseId], DirUuid).

