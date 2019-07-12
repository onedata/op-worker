%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This model holds information about single QoS, that is QoS requirement
%%% defined by the user for file or directory through QoS expression and
%%% number of required replicas. Each such requirement creates new qos_item
%%% document even if expressions are exactly the same. For each file / directory
%%% multiple qos_item can be defined.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_item).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([get/1, delete/1, create/2, update/2, set_traverse_task_ongoing/1,
    set_traverse_task_finished/1, get_file_guid/1, set_status/2, get_status/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type id() :: binary().
-type task_id() :: binary().
-type key() :: datastore:key().
-type record() :: #qos_item{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type status() :: ?FULFILLED | ?IN_PROGRESS | ?IMPOSSIBLE | undefined.
-type traverse_task_status() :: ?TRAVERSE_TASK_ONGOING_STATUS|
                                ?TRAVERSE_TASK_FINISHED_STATUS | undefined.
-type replicas_num() :: pos_integer().

-export_type([id/0, task_id/0, status/0, traverse_task_status/0, replicas_num/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Creates qos item document.
%% @end
%%--------------------------------------------------------------------
-spec create(doc(), od_space:id()) -> {ok, doc()} | {error, term()}.
create(#document{value = QosItem}, SpaceId) ->
    datastore_model:create(?CTX, #document{scope = SpaceId, value = QosItem}).

%%--------------------------------------------------------------------
%% @doc
%% Updates qos item.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Returns qos.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(QosId) ->
    datastore_model:get(?CTX, QosId).

%%--------------------------------------------------------------------
%% @doc
%% Deletes qos item.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(QosId) ->
    datastore_model:delete(?CTX, QosId).

%%%===================================================================
%%% Higher-level functions operating on qos_item record.
%%%===================================================================

-spec set_traverse_task_ongoing(id()) -> {ok, key()} | {error, term}.
set_traverse_task_ongoing(QosId) ->
    Diff = fun(QosRecord = #qos_item{traverse_task_status = TaskStatus}) ->
        case TaskStatus of
            ?TRAVERSE_TASK_ONGOING_STATUS ->
                {error, already_exists};
            _ ->
                {ok, QosRecord#qos_item{traverse_task_status = ?TRAVERSE_TASK_ONGOING_STATUS}}
        end
    end,
    update(QosId, Diff).

-spec set_traverse_task_finished(id()) -> {ok, key()} | {error, term}.
set_traverse_task_finished(QosId) ->
    Diff = fun(QosRecord = #qos_item{traverse_task_status = TaskStatus}) ->
        case TaskStatus of
            ?TRAVERSE_TASK_ONGOING_STATUS ->
                {ok, QosRecord#qos_item{traverse_task_status = ?TRAVERSE_TASK_FINISHED_STATUS}};
            _ ->
                {error, not_found}
        end
    end,
    update(QosId, Diff).

-spec get_file_guid(id()) -> file_id:file_guid().
get_file_guid(QosId) ->
    {ok, #document{value = QosItem}} = qos_item:get(QosId),
    QosItem#qos_item.file_guid.

-spec set_status(id(), status()) -> {ok, key()} | {error, term}.
set_status(QosId, Status) ->
    Diff = fun(QosItem) ->
        {ok, QosItem#qos_item{status = Status}}
    end,
    update(QosId, Diff).

-spec get_status(id()) -> status().
get_status(QosId) ->
    {ok, #document{value = QosItem}} = qos_item:get(QosId),
    QosItem#qos_item.status.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_guid, string},
        {expression, [string]},
        {replicas_num, integer},
        {status, atom},
        {traverse_task_status, atom}
    ]}.
