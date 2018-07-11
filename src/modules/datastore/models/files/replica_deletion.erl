%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements model that is used for communication between
%%% providers that take part in file replica deletion procedure.
%%% Changes in the model will appear in the remote provider via DBSync.
%%% All functions responsible for handling changes of this model are in
%%% replica_deletion_changes module.
%%% Replica deletion requests are scheduled by replica_deletion_master
%%% which is used to limit the number of simultaneous requests.
%%% Communication flow is present on the below example.
%%% Let's assume that we have providers P1 and P2, both supporting
%%% the same space.
%%% 1. replica_deletion_master:enqueue is called on P1 as the result of
%%%    autocleaning or replica_eviction
%%% 2. Request for support of replica_deletion is sent to P2.
%%%    replica_deletion_master calls ?MODULE:request which created
%%%    replica_deletion document and sets action=request.
%%% 3. P2 handles change of the document:
%%%     1) if it can support deletion, it acquires write lock for given
%%%        file and replies by calling ?MODULE:confirm
%%%     2) otherwise it refuses by calling ?MODULE:refuse
%%% 4. P1 handles reply from P2
%%%     1) If P2 confirmed, task for deleting blocks from storage is cast to
%%%        replica_deletion_workers_pool
%%% 5. P1 calls ?MODULE:release_supporting_lock to notify P2 that it
%%%    can release the lock
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    request/7,
    confirm/2,
    refuse/1,
    release_supporting_lock/1,
    delete/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type id() :: binary().
-type record() :: #replica_deletion{}.
-type doc() :: datastore_doc:doc(record()).
-type action() :: request | confirm | refuse | release_lock.
-type diff() :: datastore_doc:diff(record()).
-type type() :: autocleaning | invalidation.
-type report_id() :: autocleaning:id() | transfer:id().
-type result() :: {ok, non_neg_integer()} | {error, term()}.

-export_type([id/0, type/0, report_id/0, result/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Sends message requesting invalidation support.
%% @end
%%-------------------------------------------------------------------
-spec request(file_meta:uuid(), fslogic_blocks:blocks(),
    version_vector:version_vector(), od_provider:id(), od_space:id(),
    type(), report_id()) -> {ok, id()} | {error, term()}.
request(FileUuid, FileBlocks, VV, Requestee, SpaceId, Type, Id) ->
    NewDoc = new_doc(FileUuid, FileBlocks, VV, Requestee, SpaceId, Type, Id),
    ?extract_key(datastore_model:save(?CTX, NewDoc)).

%%-------------------------------------------------------------------
%% @doc
%% Sends message confirming invalidation support.
%% @end
%%-------------------------------------------------------------------
-spec confirm(id(), fslogic_blocks:blocks()) -> ok.
confirm(Id, Blocks) ->
    {ok, _} = update(Id, fun(ReplicaDeletion) ->
        {ok, ReplicaDeletion#replica_deletion{
            action = confirm,
            supported_blocks = Blocks
        }}
    end),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Sends message refusing invalidation support.
%% @end
%%-------------------------------------------------------------------
-spec refuse(id()) -> ok.
refuse(Id) ->
    {ok, _} = update(Id, fun(ReplicaDeletion) ->
        {ok, update_action(ReplicaDeletion, refuse)}
    end),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Sends message allowing to release deletion supporting lock.
%% @end
%%-------------------------------------------------------------------
-spec release_supporting_lock(id()) -> ok.
release_supporting_lock(Id) ->
    {ok, _} = update(Id, fun(ReplicaDeletion) ->
        {ok, update_action(ReplicaDeletion, release_lock)}
    end),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% @equiv datastore_model:delete(?CTX, Id).
%% @end
%%-------------------------------------------------------------------
-spec delete(id()) -> ok.
delete(Id) ->
    datastore_model:delete(?CTX, Id).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv datastore_model:update(?CTX, Id, Diff).
%% @end
%%-------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Id, Diff) ->
    datastore_model:update(?CTX, Id, Diff).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv datastore_model:update(?CTX, Id, Diff).
%% @end
%%-------------------------------------------------------------------
-spec new_doc(file_meta:uuid(), fslogic_blocks:blocks(),
    version_vector:version_vector(), od_provider:id(), od_space:id(),
    type(), report_id()) -> doc().
new_doc(FileUuid, FileBlocks, VV, Requestee, SpaceId, Type, Id) ->
    #document{
        value = #replica_deletion{
            file_uuid = FileUuid,
            space_id = SpaceId,
            action = request,
            requested_blocks = FileBlocks,
            version_vector = VV,
            requester = oneprovider:get_id(),
            requestee = Requestee,
            type = Type,
            report_id = Id
        },
        scope = SpaceId
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates action field in #invalidation_msg{} record.
%% @end
%%-------------------------------------------------------------------
-spec update_action(record(), action()) -> record().
update_action(ReplicaDeletion, NewStatus) ->
    ReplicaDeletion#replica_deletion{action = NewStatus}.

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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {status, atom},
        {requested_blocks, [
            {record, [
                {offset, integer},
                {size, integer}
            ]}
        ]},
        {supported_blocks, [
            {record, [
                {offset, integer},
                {size, integer}
            ]}
        ]},
        {version_vector, #{term => integer}},
        {requester, string},
        {requestee, string},
        {doc_id, string},
        {type, atom}
    ]}.