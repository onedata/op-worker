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
-module(fslogic_event).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_update/2, emit_file_sizeless_attrs_update/1,
    emit_file_location_update/2, emit_file_location_update/3,
    emit_permission_changed/1, emit_file_removal/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_update(FileEntry, ExcludedSessions) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:to_file_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{size = Size} = FileAttr} ->
            ?debug("Sending new attributes for file ~p to all sessions except ~p, size ~p",
                [FileEntry, ExcludedSessions, Size]),
            event:emit(#event{object = #update_event{object = FileAttr}}, {exclude, ExcludedSessions});
        {error, Reason} ->
            ?error("Unable to get new attributes for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_sizeless_attrs_update(fslogic_worker:file()) ->
    ok | {error, Reason :: term()}.
emit_file_sizeless_attrs_update(FileEntry) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:to_file_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{} = FileAttr} ->
            ?debug("Sending new times for file ~p to all subscribers", [FileEntry]),
            SizelessFileAttr = FileAttr#file_attr{size = undefined},
            event:emit(#event{object = #update_event{object = SizelessFileAttr}});
        {error, Reason} ->
            ?error("Unable to get new times for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @equiv emit_file_location_update(FileEntry, ExcludedSessions, undefined)
%%--------------------------------------------------------------------
-spec emit_file_location_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_update(FileEntry, ExcludedSessions) ->
    emit_file_location_update(FileEntry, ExcludedSessions, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range is requested,
%% so we may fill the gaps within, id defaults to whole file.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_update(fslogic_worker:file(), [session:id()], fslogic_blocks:block() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_update(FileEntry, ExcludedSessions, Range) ->
    try
        % get locations
        {ok, #document{} = File} = file_meta:get(FileEntry),
        {ok, LocationIds} = file_meta:get_locations(File),
        Locations = lists:map(
            fun(LocId) ->
                {ok, Location} = file_location:get(LocId),
                Location
            end, LocationIds),
        [FileLocationDoc = #document{value = FileLocation = #file_location{blocks = Blocks, uuid = FileUuid, size = Size}}] =
            lists:filter(
                fun(#document{value = #file_location{provider_id = ProviderId}}) ->
                    ProviderId =:= oneprovider:get_provider_id()
                end, Locations),

        % find gaps
        AllRanges = lists:foldl(
            fun(#document{value = #file_location{blocks = Blocks}}, Acc) ->
                fslogic_blocks:merge(Acc, Blocks)
            end, [], Locations),
        RequestedRange = utils:ensure_defined(Range, undefined, #file_block{offset = 0, size = Size}),
        ExtendedRequestedRange = case RequestedRange of
            #file_block{offset = O, size = S} when O + S < Size ->
                RequestedRange#file_block{size = Size - O};
            _ -> RequestedRange
        end,
        FullFile = replica_updater:fill_blocks_with_storage_info(
            [ExtendedRequestedRange], FileLocationDoc),
        Gaps = fslogic_blocks:consolidate(
            fslogic_blocks:invalidate(FullFile, AllRanges)
        ),
        BlocksWithFilledGaps = fslogic_blocks:merge(Blocks, Gaps),

        % fill gaps, fill storage info, transform uid and emit
        LocationToSend = file_location:ensure_blocks_not_empty(
            FileLocation#file_location{
                uuid = fslogic_uuid:to_file_guid(FileUuid),
                blocks = BlocksWithFilledGaps
            }),
        event:emit(#event{object = #update_event{
            object = LocationToSend}},
            {exclude, ExcludedSessions})
    catch
        _:Reason ->
            ?error_stacktrace("Unable to push new location for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_permission_changed(FileUuid :: file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
emit_permission_changed(FileUuid) ->
    event:emit(#event{object = #permission_changed_event{file_uuid = fslogic_uuid:to_file_guid(FileUuid)}}).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removal(FileGUID :: fslogic_worker:file_guid()) ->
    ok | {error, Reason :: term()}.
emit_file_removal(FileGUID) ->
    event:emit(#event{object = #file_removal_event{file_uuid = FileGUID}}).

%%%===================================================================
%%% Internal functions
%%%===================================================================