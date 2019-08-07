%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%% QoS is determined fulfilled when there is:
%%%     - traverse task triggered by created this QoS entry is finished
%%%     - there are no remaining transfers, that were created to fulfill this QoS
%%%
%%% Active transfers are stored as links where value is transfer id and key is
%%% expressed as combined:
%%%     * relative path to QoS entry origin file(file that QoS entry was added to)
%%%     * storage id where file is transferred to
%%%
%%% QoS status of directory is checked by getting next status link to
%%% given directory relative path. Because status links keys start with relative
%%% path, if there is unfinished transfer in subtree of this directory next
%%% status link will start with the same relative path.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([add_status_link/5, delete_status_link/4, check_fulfilment/2, get_relative_path/2]).

-type path() :: binary().

-define(QOS_STATUS_LINK_NAME(RelativePath, StorageId), <<RelativePath/binary, "###", StorageId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds new status link for given qos.
%% @end
%%--------------------------------------------------------------------
-spec add_status_link(qos_entry:id(), datastore_doc:scope(), path(), storage:id(),
    transfer:id()) ->  ok | {error, term()}.
add_status_link(QosId, Scope, RelativePath, StorageId, TransferId) ->
    Link = {?QOS_STATUS_LINK_NAME(RelativePath, StorageId), TransferId},
    {ok, _} = qos_entry:add_links(Scope, QosId, oneprovider:get_id(), Link).

%%--------------------------------------------------------------------
%% @doc
%% Deletes given status link from given qos.
%% @end
%%--------------------------------------------------------------------
-spec delete_status_link(qos_entry:id(), datastore_doc:scope(), path(), storage:id()) ->
    ok | {error, term()}.
delete_status_link(QosId, Scope, RelativePath, StorageId) ->
    ok = qos_entry:delete_links(Scope, QosId, oneprovider:get_id(),
        ?QOS_STATUS_LINK_NAME(RelativePath, StorageId)).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether given QoS is fulfilled for given file i.e. there is no traverse task
%% and all transfers in subtree of given file are finished.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfilment(qos_entry:id(), fslogic_worker:file_guid()) ->  boolean().
check_fulfilment(QosId, FileGuid) ->
    {ok, #document{value = QosItem}} = qos_entry:get(QosId),
    check_fulfilment_internal(QosId, FileGuid, QosItem).

%%--------------------------------------------------------------------
%% @doc
%% TODO VFS-5633 use uuid instead of filename
%% Returns child file path relative to ancestor's, e.g:
%%    AncestorAbsolutePath: space1/dir1/dir2
%%    ChildAbsolutePath: space1/dir1/dir2/dir3/file
%%    Result: dir2/dir3/file
%% @end
%%--------------------------------------------------------------------
-spec get_relative_path([binary()] | file_meta:uuid(), fslogic_worker:guid()) -> path().
get_relative_path(AncestorPathTokens, ChildGuid) when is_list(AncestorPathTokens) ->
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(ChildGuid)),
    filename:join(lists:sublist(FilePathTokens, length(AncestorPathTokens), length(FilePathTokens)));
get_relative_path(AncestorGuid, ChildGuid) ->
    {AncestorPathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(AncestorGuid)),
    get_relative_path(AncestorPathTokens, ChildGuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec check_fulfilment_internal(qos_entry:id(), fslogic_worker:file_guid(), qos_entry:record()) ->  boolean().
check_fulfilment_internal(QosId, FileGuid, #qos_entry{file_uuid = OriginGuid, status = Status}) ->
    case Status of
        ?QOS_TRAVERSE_FINISHED_STATUS ->
            RelativePath = get_relative_path(OriginGuid, FileGuid),
            case get_next_status_link(QosId, RelativePath) of
                {ok, empty} ->
                    true;
                {ok, Path} ->
                    not str_utils:binary_starts_with(Path, RelativePath)
            end;
        _ ->
            %TODO VFS-5642 check if subtree was already traversed
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns next status link.
%% @end
%%--------------------------------------------------------------------
-spec get_next_status_link(qos_entry:id(), binary()) ->  {ok, path()} | {error, term()}.
get_next_status_link(QosId, PrevName) ->
    datastore_model:fold_links(qos_entry:get_ctx(), QosId, all,
        fun(#link{name = N}, _Acc) -> {ok, N} end,
        empty,
        #{prev_link_name => PrevName, size => 1}
    ).

