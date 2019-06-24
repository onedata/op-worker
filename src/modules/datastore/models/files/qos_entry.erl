%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This model holds information about single QoS, that is QoS requirement
%%% defined by the user for file or directory through QoS expression and
%%% number of required replicas. Each such requirement creates new qos_entry
%%% document even if expressions are exactly the same. For each file / directory
%%% multiple qos_entry can be defined.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry).
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
-export([get/1, delete/1, create/2, update/2,
    add_status_link/3, delete_status_link/3,
    add_impossible_qos/2, list_impossible_qos/0,
    get_file_guid/1, check_fulfilment/3,
    set_status/2, get_status/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type id() :: binary().
-type key() :: datastore:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type status() :: ?QOS_IN_PROGRESS_STATUS | ?QOS_TRAVERSE_FINISHED_STATUS | ?QOS_IMPOSSIBLE_STATUS.
-type replicas_num() :: pos_integer().

-export_type([id/0, status/0, replicas_num/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined(),
    fold_enabled => true
}).

-export([list/0]).

%%%===================================================================
%%% API
%%%===================================================================

list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Creates qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec create(doc(), od_space:id()) -> {ok, doc()} | {error, term()}.
create(#document{value = QosEntry}, SpaceId) ->
    datastore_model:create(?CTX, #document{scope = SpaceId, value = QosEntry}).

%%--------------------------------------------------------------------
%% @doc
%% Updates qos_entry.
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
%% Deletes qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(QosId) ->
    datastore_model:delete(?CTX, QosId).

%%%===================================================================
%%% Higher-level functions operating on qos_entry record.
%%%===================================================================

-spec get_file_guid(id()) -> file_id:file_guid().
get_file_guid(QosId) ->
    {ok, #document{value = QosEntry}} = qos_entry:get(QosId),
    QosEntry#qos_entry.file_guid.

-spec set_status(id(), status()) -> {ok, key()} | {error, term}.
set_status(QosId, Status) ->
    Diff = fun(QosEntry) ->
        {ok, QosEntry#qos_entry{status = Status}}
    end,
    update(QosId, Diff).

-spec get_status(id()) -> status() | {error, term}.
get_status(QosId) ->
    {ok, #document{value = QosEntry}} = qos_entry:get(QosId),
    QosEntry#qos_entry.status.

%%--------------------------------------------------------------------
%% @doc
%% Adds new status link for given qos. Name should be relative path to qos root.
%% This links are necessary to calculate qos status.
%% @end
%%--------------------------------------------------------------------
-spec add_status_link(id(), datastore_doc:scope(), binary()) ->  ok | {error, term()}.
add_status_link(QosId, Scope, Name) ->
    Ctx = ?CTX#{scope => Scope},
    Link = {Name, Name},
    ?extract_ok(datastore_model:add_links(Ctx, QosId, oneprovider:get_id(), Link)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes given status link from given qos.
%% @end
%%--------------------------------------------------------------------
-spec delete_status_link(id(), datastore_doc:scope(), binary()) ->  ok | {error, term()}.
delete_status_link(QosId, Scope, LinkName) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, QosId, oneprovider:get_id(), LinkName).

%%--------------------------------------------------------------------
%% @doc
%% Adds given QosId to impossible qos tree.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(id(), datastore_doc:scope()) ->  ok | {error, term()}.
add_impossible_qos(QosId, Scope) ->
    qos_entry:update(QosId, fun(QosItem) ->
        {ok, QosItem#qos_entry{status = impossible}}
    end),
    datastore_model:add_links(?CTX#{scope => Scope}, ?IMPOSSIBLE_QOS_KEY, oneprovider:get_id(), {QosId, QosId}).

%%--------------------------------------------------------------------
%% @doc
%% Lists all impossible qos.
%% @end
%%--------------------------------------------------------------------
-spec list_impossible_qos() ->  {ok, [id()]} | {error, term()}.
list_impossible_qos() ->
    datastore_model:fold_links(?CTX, ?IMPOSSIBLE_QOS_KEY, all,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether given qos is fulfilled for given file i.e. there is no traverse task and all transfers are finished.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfilment(id(), fslogic_worker:file_guid() | undefined, #qos_entry{}) ->  boolean().
check_fulfilment(QosId, undefined, #qos_entry{file_guid = FileUuid} = QosItem) ->
    check_fulfilment(QosId, fslogic_uuid:uuid_to_guid(FileUuid), QosItem);
check_fulfilment(QosId, FileGuid, #qos_entry{file_guid = OriginUuid, status = Status}) ->
    case Status of
        ?QOS_TRAVERSE_FINISHED_STATUS ->
            RelativePath = fslogic_path:get_relative_path(OriginUuid, FileGuid),
            case get_next_status_link(QosId, RelativePath) of
                {ok, []} ->
                    true;
                {ok, [Path]} ->
                    not str_utils:binary_starts_with(Path, RelativePath);
                _ ->
                    false
            end;
        _ ->
            false
    end.


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
        {status, atom}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns next status link.
%% @end
%%--------------------------------------------------------------------
-spec get_next_status_link(id(), binary()) ->  {ok, [binary()]} | {error, term()}.
get_next_status_link(QosId, PrevName) ->
    datastore_model:fold_links(?CTX, QosId, all,
        fun(#link{name = N}, Acc) -> {ok, [N | Acc]} end,
        [],
        #{prev_link_name => PrevName, size => 1}
    ).

