%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module responsible for pushing new file's information to sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_event_emitter).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_changed/2, emit_file_attr_changed/3,
    emit_file_attr_changed_with_replication_status/3, emit_sizeless_file_attrs_changed/1,
    emit_file_location_changed/2, emit_file_location_changed/3,
    emit_file_location_changed/4, emit_file_locations_changed/2,
    emit_file_perm_changed/1, emit_file_removed/2,
    emit_file_renamed_no_exclude/5, emit_file_renamed_to_client/5, emit_quota_exceeded/0,
    emit_helper_params_changed/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% Should be used when action that generated event does not result in changes of blocks.
%% Otherwise emit_file_attr_changed_with_replication_status function should be used.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileCtx, ExcludedSessions) ->
    case file_ctx:get_and_cache_file_doc_including_deleted(FileCtx) of
        {error, not_found} ->
            ok;
        {#document{}, FileCtx2} ->
            RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
            {#fuse_response{
                fuse_response = #file_attr{} = FileAttr
            }, ConflictingFiles, _} = attr_req:get_file_attr_and_conflicts_insecure(RootUserCtx, FileCtx2, #{
                allow_deleted_files => true,
                include_size => true,
                name_conflicts_resolution_policy => resolve_name_conflicts
            }),
            emit_suffixes(ConflictingFiles, {ctx, FileCtx2}),
            emit_file_attr_changed(FileCtx2, FileAttr, ExcludedSessions);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed(file_ctx:ctx(), #file_attr{}, [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileCtx, FileAttr, ExcludedSessions) ->
    event:emit_to_filtered_subscribers(#file_attr_changed_event{file_attr = FileAttr},
        #{file_ctx => FileCtx}, ExcludedSessions).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all except for the ones present
%% in 'ExcludedSessions' list.
%% Includes replication status to those who request it.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed_with_replication_status(file_ctx:ctx(), boolean(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed_with_replication_status(FileCtx, SizeChanged, ExcludedSessions) ->
    case file_ctx:get_and_cache_file_doc_including_deleted(FileCtx) of
        {error, not_found} ->
            ok;
        {#document{}, FileCtx2} ->
            case subscription_manager:get_attr_event_subscribers(
                file_ctx:get_guid_const(FileCtx2), #{file_ctx => FileCtx2}, SizeChanged) of
                [{ok, WithoutStatusSessIds}, {ok, WithStatusSessIds}] ->
                    emit_file_attr_changed_with_replication_status_internal(FileCtx2,
                        WithoutStatusSessIds -- ExcludedSessions, WithStatusSessIds -- ExcludedSessions);
                [{ok, WithStatusSessIds}] ->
                    emit_file_attr_changed_with_replication_status_internal(FileCtx2, [],
                        WithStatusSessIds -- ExcludedSessions);
                [{error, Reason}, _] ->
                    {error, Reason};
                [_, {error, Reason2}] ->
                    {error, Reason2}
            end;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_sizeless_file_attrs_changed(file_ctx:ctx()) ->
    ok | {error, Reason :: term()}.
emit_sizeless_file_attrs_changed(FileCtx) ->
    case file_ctx:get_and_cache_file_doc_including_deleted(FileCtx) of
        {error, not_found} ->
            ok;
        {#document{}, FileCtx2} ->
            RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
            #fuse_response{
                fuse_response = #file_attr{} = FileAttr
            } = attr_req:get_file_attr_insecure(RootUserCtx, FileCtx2, #{
                allow_deleted_files => true,
                include_size => false,
                name_conflicts_resolution_policy => resolve_name_conflicts
            }),
            event:emit_to_filtered_subscribers(#file_attr_changed_event{
                file_attr = FileAttr
            }, #{file_ctx => FileCtx2}, []);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv emit_file_location_changed(FileEntry, ExcludedSessions, undefined)
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions) ->
    emit_file_location_changed(FileCtx, ExcludedSessions, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range is requested,
%% so we may fill the gaps within, id defaults to whole file.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()],
    fslogic_blocks:blocks() | fslogic_blocks:block() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions, Range) ->
    {Location, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
    {Offset, Size} = fslogic_location_cache:get_blocks_range(Location, Range),
    emit_file_location_changed(Location, ExcludedSessions, Offset, Size).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range specifies which
%% range of blocks should be included in event.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_location:record(), [session:id()],
    non_neg_integer() | undefined, non_neg_integer() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(Location, ExcludedSessions, Offset, OffsetEnd) ->
    event:emit(create_file_location_changed(Location, Offset, OffsetEnd),
        {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends file location changes to all subscribers except for the ones
%% present in 'ExcludedSessions' list. It is faster than execution of
%% emit_file_location_changed on each change.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_locations_changed(replica_updater:location_changes_description(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_locations_changed([], _ExcludedSessions) ->
    ok;
emit_file_locations_changed(LocationChangesDescription, ExcludedSessions) ->
    EventsList = lists:map(fun({Location, Offset, OffsetEnd}) ->
        create_file_location_changed(Location, Offset, OffsetEnd)
    end, LocationChangesDescription),
    event:emit({aggregated, EventsList}, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_perm_changed(file_ctx:ctx()) -> ok | {error, Reason :: term()}.
emit_file_perm_changed(FileCtx) ->
    event:emit(#file_perm_changed_event{
        file_guid = file_ctx:get_guid_const(FileCtx)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removed(file_ctx:ctx(), ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_removed(FileCtx, ExcludedSessions) ->
    Ans = event:emit_to_filtered_subscribers(#file_removed_event{file_guid = file_ctx:get_guid_const(FileCtx)},
        #{file_ctx => FileCtx}, ExcludedSessions),
    {Doc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    case file_meta:check_name_and_get_conflicting_files(Doc) of
        {conflicting, _, ConflictingFiles} ->
            emit_suffixes(ConflictingFiles, {ctx, FileCtx2});
        _ ->
            ok
    end,

    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing given client about file rename.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed_to_client(file_ctx:ctx(), fslogic_worker:file_guid(), file_meta:name(),
    file_meta:name(), user_ctx:ctx()) -> ok | {error, Reason :: term()}.
emit_file_renamed_to_client(FileCtx, NewParentGuid, NewName, PrevName, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {OldParentGuid, _FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    emit_file_renamed(FileCtx, OldParentGuid, NewParentGuid, NewName, PrevName, [SessionId]).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing given client about file rename. No sessions are excluded.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed_no_exclude(file_ctx:ctx(), fslogic_worker:file_guid(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:name()) -> ok | {error, Reason :: term()}.
emit_file_renamed_no_exclude(FileCtx, OldParentGuid, NewParentGuid, NewName, PrevName) ->
    emit_file_renamed(FileCtx, OldParentGuid, NewParentGuid, NewName, PrevName, []).

%%--------------------------------------------------------------------
%% @doc
%% Sends a list of currently disabled spaces due to exceeded quota.
%% @end
%%--------------------------------------------------------------------
-spec emit_quota_exceeded() -> ok | {error, Reason :: term()}.
emit_quota_exceeded() ->
    case space_quota:get_disabled_spaces() of
        {ok, BlockedSpaces} ->
            ?debug("Sending disabled spaces event ~p", [BlockedSpaces]),
            event:emit(#quota_exceeded_event{spaces = BlockedSpaces});
        {error, _} = Error ->
            ?debug("Cannot send disabled spaces event due to ~p", [Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends event indicating storage helper params have changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_helper_params_changed(StorageId :: storage:id()) ->
    ok | {error, Reason :: term()}.
emit_helper_params_changed(StorageId) ->
    event:emit(#helper_params_changed_event{
        storage_id = StorageId
    }).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Helper function for sending current file attributes including replication status.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed_with_replication_status_internal(file_ctx:ctx(), [session:id()], [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed_with_replication_status_internal(_FileCtx, [], []) ->
    ok;
emit_file_attr_changed_with_replication_status_internal(FileCtx, WithoutStatusSessIds, WithStatusSessIds) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {#fuse_response{fuse_response = #file_attr{} = FileAttr}, ConflictingFiles, _} =
        attr_req:get_file_attr_and_conflicts_insecure(RootUserCtx, FileCtx, #{
            allow_deleted_files => true,
            include_size => true,
            name_conflicts_resolution_policy => resolve_name_conflicts,
            include_replication_status => WithStatusSessIds =/= []
        }),
    emit_suffixes(ConflictingFiles, {ctx, FileCtx}),
    event:emit(#file_attr_changed_event{file_attr = FileAttr}, WithStatusSessIds),
    event:emit(#file_attr_changed_event{file_attr = FileAttr#file_attr{fully_replicated = undefined}},
        WithoutStatusSessIds -- WithStatusSessIds).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing given client about file rename.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed(file_ctx:ctx(), fslogic_worker:file_guid(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:name(), [session:id()]) -> ok | {error, Reason :: term()}.
emit_file_renamed(FileCtx, OldParentGuid, NewParentGuid, NewName, OldName, Exclude) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    {Doc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    ProviderId = file_meta:get_provider_id(Doc),
    {ok, FileUuid} = file_meta:get_uuid(Doc),
    % do not get ParentUuid and Name from Doc as the doc contains already updated information
    FinalName = case file_meta:check_name_and_get_conflicting_files(file_id:guid_to_uuid(OldParentGuid), 
        OldName, FileUuid, ProviderId
    ) of
        {conflicting, ExtendedName, ConflictingFiles} ->
            emit_suffixes(ConflictingFiles, {parent_guid, NewParentGuid}),
            ExtendedName;
        _ ->
            NewName
    end,

    event:emit_to_filtered_subscribers(#file_renamed_event{top_entry = #file_renamed_entry{
        old_guid = Guid,
        new_guid = Guid,
        new_parent_guid = NewParentGuid,
        new_name = FinalName
    }}, [#{file_ctx => FileCtx2, parent => OldParentGuid},
        #{file_ctx => FileCtx2, parent => NewParentGuid}], Exclude).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates #file_location_changed_event.
%% @end
%%--------------------------------------------------------------------
-spec create_file_location_changed(file_location:record(),
    non_neg_integer() | undefined, non_neg_integer() | undefined) ->
    #file_location_changed_event{}.
create_file_location_changed(Location, Offset, OffsetEnd) ->
    #file_location_changed_event{
        file_location = Location,
        change_beg_offset = Offset, change_end_offset = OffsetEnd
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits #file_renamed_event for files that can be viewed with different suffix
%% after changes represented by other event.
%% The function is introduced because oneclient does not resolve conflicts on its side.
%% Can be removed after upgrade of oneclient.
%% @end
%%--------------------------------------------------------------------
-spec emit_suffixes(Files :: file_meta:conflicts(),
    {parent_guid, ParentGuid :: fslogic_worker:file_guid()} | {ctx, file_ctx:ctx()}) -> ok.
emit_suffixes([], _) ->
    ok;
emit_suffixes(ConflictingFiles, {parent_guid, ParentGuid}) ->
    lists:foreach(fun({TaggedName, Uuid}) ->
        try
            {_, SpaceId} = file_id:unpack_guid(ParentGuid),
            Guid = file_id:pack_guid(Uuid, SpaceId),
            FileCtx = file_ctx:new_by_guid(Guid),

            event:emit_to_filtered_subscribers(#file_renamed_event{top_entry = #file_renamed_entry{
                old_guid = Guid,
                new_guid = Guid,
                new_parent_guid = ParentGuid,
                new_name = TaggedName
            }}, [#{file_ctx => FileCtx, parent => ParentGuid}], [])
        catch
            _:_ -> ok % File not fully synchronized (file_meta is missing)
        end
    end, ConflictingFiles);
emit_suffixes(Files, {ctx, FileCtx}) ->
    try
        {ParentCtx, _} = file_ctx:get_parent(FileCtx, user_ctx:new(?ROOT_USER_ID)),
        emit_suffixes(Files, {parent_guid, file_ctx:get_guid_const(ParentCtx)})
    catch
        _:_ -> ok % Parent not fully synchronized
    end.