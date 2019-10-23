%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%% QoS is fulfilled when:
%%%     - all traverse tasks, triggered by creating this QoS entry, are finished
%%%     - there are no remaining transfers, that were created to fulfill this QoS
%%%
%%% Active transfers are stored as links where value is transfer id and key is
%%% expressed as combined:
%%%     * relative path to QoS entry origin file (file that QoS entry was added to)
%%%     * storage id where file is transferred to
%%%     * traverse task id that started this transfer
%%%
%%% QoS status of directory is checked by getting next status link to
%%% given directory relative path. Because status links keys start with relative
%%% path, if there is unfinished transfer in subtree of this directory next
%%% status link will start with given parent directory relative path.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/qos.hrl").

%% API
-export([report_file_changed/4, report_file_reconciled/4, check_fulfilment/2]).

-type path() :: binary().

-define(QOS_STATUS_LINK_NAME(RelativePath, TaskId),
    <<RelativePath/binary, "###", TaskId/binary>>).

-define(QOS_STATUS_LINKS_KEY(QosEntryId), <<"qos_status", QosEntryId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new link indicating that requirements for given qos_entry are not
%% fulfilled as file has been changed.
%% @end
%%--------------------------------------------------------------------
-spec report_file_changed(qos_entry:id(), datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_changed(QosEntryId, SpaceId, FileUuid, TaskId) ->
    {ok, OriginFileGuid} = qos_entry:get_file_guid(QosEntryId),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = get_relative_path(OriginFileGuid, FileGuid),
    Link = {?QOS_STATUS_LINK_NAME(RelativePath, TaskId), TaskId},
    {ok, _} = qos_entry:add_links(SpaceId, ?QOS_STATUS_LINKS_KEY(QosEntryId), oneprovider:get_id(), Link),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Removes link indicating that requirements for given qos_entry are not
%% fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec report_file_reconciled(qos_entry:id(), datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_reconciled(QosEntryId, SpaceId, FileUuid, TaskId) ->
    {ok, OriginFileGuid} = qos_entry:get_file_guid(QosEntryId),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = get_relative_path(OriginFileGuid, FileGuid),
    ok = qos_entry:delete_links(SpaceId, ?QOS_STATUS_LINKS_KEY(QosEntryId), oneprovider:get_id(),
        ?QOS_STATUS_LINK_NAME(RelativePath, TaskId)).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether given QoS is fulfilled for given file i.e. there is no traverse task
%% and all transfers in subtree of given file are finished.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfilment(qos_entry:id(), fslogic_worker:file_guid()) ->  boolean().
check_fulfilment(QosEntryId, FileGuid) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),

    case qos_entry:is_possible(QosDoc) of
        false -> false;
        true ->
            case qos_entry:are_all_traverses_finished(QosDoc) of
                true ->
                    {ok, OriginGuid} = qos_entry:get_file_guid(QosDoc),
                    RelativePath = get_relative_path(OriginGuid, FileGuid),
                    case get_next_status_link(QosEntryId, RelativePath) of
                        {ok, empty} ->
                            true;
                        {ok, Path} ->
                            not str_utils:binary_starts_with(Path, RelativePath)
                    end;
                false ->
                    %TODO VFS-5642 check if subtree was already traversed
                    false
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% TODO VFS-5633 use uuid instead of filename
%% Returns child file path relative to ancestor's, e.g:
%%    AncestorAbsolutePath: space1/dir1/dir2
%%    ChildAbsolutePath: space1/dir1/dir2/dir3/file
%%    Result: dir2/dir3/file
%% @end
%%--------------------------------------------------------------------
-spec get_relative_path([binary()] | file_meta:uuid(), fslogic_worker:file_guid()) -> path().
get_relative_path(AncestorPathTokens, ChildGuid) when is_list(AncestorPathTokens) ->
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(ChildGuid)),
    filename:join(lists:sublist(FilePathTokens, length(AncestorPathTokens), length(FilePathTokens)));
get_relative_path(AncestorGuid, ChildGuid) ->
    {AncestorPathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(AncestorGuid)),
    get_relative_path(AncestorPathTokens, ChildGuid).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns next status link in alphabetical order.
%% @end
%%--------------------------------------------------------------------
-spec get_next_status_link(qos_entry:id(), binary()) ->  {ok, path() | empty} | {error, term()}.
get_next_status_link(QosEntryId, PrevName) ->
    qos_entry:fold_links(?QOS_STATUS_LINKS_KEY(QosEntryId),
        fun(#link{name = N}, _Acc) -> {ok, N} end,
        empty,
        #{prev_link_name => PrevName, size => 1}
    ).
