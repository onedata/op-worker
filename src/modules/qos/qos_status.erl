%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%% QoS entry is fulfilled when:
%%%     - there is no information that qos_entry cannot be satisfied (see qos_entry.erl)
%%%     - there are no traverse requests in qos_entry document (see qos_entry.erl)
%%%     - there are no links indicating that file has been changed and it should
%%%       be reconciled. When provider notices file_location change that impacts
%%%       local replica it calculates effective_file_qos for given file. For
%%%       each QoS entry in effective_file_qos provider creates appropriate link
%%%       and schedules file reconciliation. Links are created as follow:
%%%         * value is set to traverse task ID
%%%         * key is expressed as combined: relative path to QoS entry origin
%%%           file (file that QoS entry was added to) and traverse task id
%%%
%%% QoS fulfillment status of directory is checked by getting next status link to
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
    {ok, _} = qos_entry:add_synced_links(SpaceId, ?QOS_STATUS_LINKS_KEY(QosEntryId), oneprovider:get_id(), Link),
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
    ok = qos_entry:delete_synced_links(SpaceId, ?QOS_STATUS_LINKS_KEY(QosEntryId), oneprovider:get_id(),
        ?QOS_STATUS_LINK_NAME(RelativePath, TaskId)).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether requirements specified in given qos_entry are fulfilled
%% for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfilment(qos_entry:id(), fslogic_worker:file_guid()) ->  boolean().
check_fulfilment(QosEntryId, FileGuid) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),

    case qos_entry:is_possible(QosDoc) of
        false -> false;
        true ->
            case qos_traverse_req:are_all_finished(AllTraverseReqs) of
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
    qos_entry:fold_links(?QOS_STATUS_LINKS_KEY(QosEntryId), all,
        fun(#link{name = N}, _Acc) -> {ok, N} end,
        empty,
        #{prev_link_name => PrevName, size => 1}
    ).
