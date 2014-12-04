%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides api which allowes to:
%% - check_if_synchronized - check if selected part of file is in sync with other providers (and also find out which provider has the newest version)
%% - mark_as_modified - mark some file part as modified, so other providers could fetch this data later
%% - mark_as_available - mark some file part as available, it means that method caller has newest version of file block, on local storage
%% - truncate - inform that file was truncated, the remote_parts ranges would fit to that new size
%% @end
%% ===================================================================
-module(fslogic_available_blocks).

-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/rtransfer/rtransfer.hrl").

% High level fslogic api
-export([synchronize_file_block/3, file_block_modified/5, file_truncated/4, db_sync_hook/0]).
% cache/dao_proxy api
-export([cast/1, call/1]).
-export([file_synchronized/6, file_block_modified/10, file_truncated/9, external_available_blocks_changed/5]).
-export([registered_requests/0, save_available_blocks/3, get_available_blocks/3, list_all_available_blocks/3, get_file_size/3, invalidate_blocks_cache/3]).
% Low level document modification api
-export([mark_as_modified/2, mark_as_available/2, check_if_synchronized/3, truncate/2, mark_other_provider_changes/2]).
% Utility functtions
-export([block_to_byte_range/2, byte_to_offset_range/1, ranges_to_offset_tuples/1, get_timestamp/0]).

% Test API
-ifdef(TEST).
-export([byte_to_block_range/1]).
-endif.

%% Defines how long in miliseconds event processing unit will wait for event with missing sequence number
-define(EVENT_SYNCH_TIMEOUT, 5000).

%% ====================================================================
%% High level functions for handling available_blocks document changes
%% ====================================================================
%% Every change should pass throught one of this functions

%% synchronize_file_block/3
%% ====================================================================
%% @doc Checks if given byte range of file's value is in sync with other providers. If not, the data is fetched from them, and stored
%% on local storage
%% @end
-spec synchronize_file_block(FullFileName :: string(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
synchronize_file_block(FullFileName, Offset, Size) ->
    ?debug("synchronize_file_block(~p,~p,~p)", [FullFileName, Offset, Size]),

    %prepare data
    {ok, #db_document{uuid = FileId}} = fslogic_objects:get_file(FullFileName), %todo cache this somehow
    {ok, RemoteLocationDocs} = call({list_all_available_blocks, FileId}),
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyRemoteLocationDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id == ProviderId end, RemoteLocationDocs),
    OtherRemoteLocationDocs = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id =/= ProviderId end, RemoteLocationDocs),

    % synchronize file
    OutOfSyncList = fslogic_available_blocks:check_if_synchronized(#offset_range{offset = Offset, size = Size}, MyRemoteLocationDoc, OtherRemoteLocationDocs),
    lists:foreach(
        fun({Id, Ranges}) ->
            lists:foreach(fun(Range = #range{from = From, to = To}) ->
                ?info("Synchronizing blocks: ~p of file ~p", [Range, FullFileName]),
                O = From * ?remote_block_size,
                S = (To - From + 1) * ?remote_block_size,
                gen_server:call(?Dispatcher_Name, {rtransfer, 1,
                    #request_transfer{file_id = FileId, offset = O, size = S, notify = self(), provider_id = Id}}),

                {ok, _} =
                    receive
                        {transfer_complete, Read, {FileId, O, S}} -> {ok, Read};
                        {transfer_error, Reason, {FileId, O, S}} -> {error, Reason}
                    after
                        timer:seconds(10) -> timeout
                    end
            end, Ranges)
        end, OutOfSyncList),
    SyncedParts = [Range || {_PrId, Range} <- OutOfSyncList], % assume that all parts has been synchronized

%%     #atom{value = ?VOK} = case SyncedParts of
%%         [] -> #atom{value = ?VOK};
%%         _ -> call({file_synchronized, fslogic_context:get_context(), FileId, SyncedParts, FullFileName}) % todo remove FullFileName arg
%%     end.
    call({file_synchronized, fslogic_context:get_context(), FileId, SyncedParts, FullFileName}).


%% file_block_modified/5
%% ====================================================================
%% @doc Marks given file block as modified, so other provider would know
%% that they need to synchronize their data
%% @end
-spec file_block_modified(FullFileName :: string(), FuseId :: string(), SequenceNumber :: non_neg_integer(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
file_block_modified(FullFileName, FuseId, SequenceNumber, Offset, Size) ->
    ?debug("file_block_modified(~p,~p,~p,~p,~p)", [FullFileName, FuseId, SequenceNumber, Offset, Size]),
    {ok, #db_document{uuid = FileId}} = fslogic_objects:get_file(FullFileName), %todo cache this somehow
    #atom{value = ?VOK} = call({file_block_modified, fslogic_context:get_context(), FileId, FuseId, SequenceNumber, Offset, Size, FullFileName}). % todo remove FullFileName arg

%% file_truncated/4
%% ====================================================================
%% @doc Deletes synchronization info of truncated blocks, so other provider would know
%% that those blocks has been deleted
%% @end
-spec file_truncated(FullFileName :: string(), FuseId :: string(), SequenceNumber :: non_neg_integer(), Size :: non_neg_integer()) ->
    #atom{} | no_return().
%% ====================================================================
file_truncated(FullFileName, FuseId, SequenceNumber, Size) ->
    ?debug("file_truncated(~p,~p,~p,~p)", [FullFileName, FuseId, SequenceNumber, Size]),
    {ok, #db_document{uuid = FileId}} = fslogic_objects:get_file(FullFileName), %todo cache this somehow
    #atom{value = ?VOK} = call({file_truncated, fslogic_context:get_context(), FileId, FuseId, SequenceNumber, Size, FullFileName}). % todo remove FullFileName arg

db_sync_hook() ->
    MyProviderId = cluster_manager_lib:get_provider_id(),
    fun
        (?FILES_DB_NAME, _, _, #db_document{record = #available_blocks{file_id = FileId}, deleted = true}) ->
            fslogic_available_blocks:call({invalidate_blocks_cache, FileId});
        (?FILES_DB_NAME, _, Uuid, #db_document{record = #available_blocks{provider_id = Id, file_id = FileId}, deleted = false}) when Id =/= MyProviderId ->
            fslogic_available_blocks:call({external_available_blocks_changed, fslogic_context:get_context(), utils:ensure_list(FileId), utils:ensure_list(Uuid)});
        (?FILES_DB_NAME, _, _, FileDoc = #db_document{uuid = FileId, record = #file{}, deleted = false}) ->
            {ok, FullFileName} = logical_files_manager:get_file_full_name_by_uuid(FileId),
            ok = fslogic_file:ensure_file_location_exists(FullFileName, FileDoc);
        (?FILES_DB_NAME, _, _, #db_document{uuid = FileUuid, record = #file{}, deleted = true}) ->
            {ok, Locations} = dao_lib:apply(dao_vfs, get_file_locations, [utils:ensure_list(FileUuid)], fslogic_context:get_protocol_version()),
            lists:foreach(
                fun(#db_document{uuid = Uuid, record = #file_location{storage_file_id = StorageFileId, storage_uuid = StorageUuid}}) ->
                    {ok, #db_document{record = Storage}} = fslogic_objects:get_storage({uuid, StorageUuid}),
                    {SH, _} = fslogic_utils:get_sh_and_id(?CLUSTER_FUSE_ID, Storage, StorageFileId),
                    ok = storage_files_manager:delete(SH, StorageFileId),
                    ok = dao_lib:apply(dao_vfs, remove_file_location, [Uuid], fslogic_context:get_protocol_version())
                end, Locations)
    end.

%% ====================================================================
%% Available blocks cache functions
%% ====================================================================
%% Cache is registered in fslogic init function, request concerning the same document
%% Are always handled by one process. This functions should not be used directly, send
%% request to fslogic_worker instead (using ?MODULE:cast/1, ?MODULE:call/1)

registered_requests() ->
    fun
        (ProtocolVersion, {save_available_blocks, Doc}, CacheName) ->
            fslogic_available_blocks:save_available_blocks(ProtocolVersion, CacheName, Doc);
        (ProtocolVersion, {get_available_blocks, FileId}, CacheName) ->
            fslogic_available_blocks:get_available_blocks(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {list_all_available_blocks, FileId}, CacheName) ->
            fslogic_available_blocks:list_all_available_blocks(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {get_file_size, FileId}, CacheName) ->
            fslogic_available_blocks:get_file_size(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {invalidate_blocks_cache, FileId}, CacheName) ->
            fslogic_available_blocks:invalidate_blocks_cache(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {file_block_modified, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName}, CacheName) ->
            fslogic_available_blocks:file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, ?EVENT_SYNCH_TIMEOUT);
        (ProtocolVersion, {file_truncated, Context, FileId, FuseId, SequenceNumber, Size, FullFileName}, CacheName) ->
            fslogic_available_blocks:file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, ?EVENT_SYNCH_TIMEOUT);
        (ProtocolVersion, {file_synchronized, Context, FileId, Ranges, FullFileName}, CacheName) ->
            fslogic_available_blocks:file_synchronized(ProtocolVersion, CacheName, Context, FileId, Ranges, FullFileName);
        (ProtocolVersion, {external_available_blocks_changed, Context, FileId, DocumentUuid}, CacheName) ->
            fslogic_available_blocks:external_available_blocks_changed(ProtocolVersion, CacheName, Context, FileId, DocumentUuid)
    end.

cast(Req) ->
    gen_server:call(?Dispatcher_Name, {fslogic, 1, Req}, ?CACHE_REQUEST_TIMEOUT).

call(Req) ->
    MsgId = make_ref(),
    gen_server:call(?Dispatcher_Name, {fslogic, 1, self(), MsgId, Req}, ?CACHE_REQUEST_TIMEOUT),
    receive
        {worker_answer, MsgId, Resp} -> Resp
    after ?CACHE_REQUEST_TIMEOUT ->
        ?error("Timeout in call to available_blocks process tree, req: ~p", [Req]),
        {error, timeout}
    end.

file_synchronized(ProtocolVersion, CacheName, Context, FileId, SyncedParts, FullFileName) ->
    fslogic_context:set_context(Context),
    {ok, RemoteLocationDocs} = list_all_available_blocks(ProtocolVersion, CacheName, FileId),
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyRemoteLocationDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id == ProviderId end, RemoteLocationDocs),

    %modify document
    NewDoc = lists:foldl(fun(Ranges, Acc) ->
        fslogic_available_blocks:mark_as_available(Ranges, Acc) end, MyRemoteLocationDoc, SyncedParts),

    % notify cache, db and fuses
    case MyRemoteLocationDoc == NewDoc of
        true -> ok;
        false ->
            %update size to newest
            {ok, FileSize} = get_file_size(ProtocolVersion, CacheName, FileId),
            FinalNewDoc = update_size(NewDoc, FileSize),

            %save available_blocks and notify fuses
            {ok, _} = save_available_blocks(ProtocolVersion, CacheName, FinalNewDoc),
            lists:foreach(fun(Ranges) ->
                {ok, _} = fslogic_req_regular:update_file_block_map(FullFileName, fslogic_available_blocks:ranges_to_offset_tuples(Ranges), false)
            end, SyncedParts)
    end,
    #atom{value = ?VOK}.

file_block_modified_internal(ProtocolVersion, CacheName, Context, FileId, FuseId, Offset, Size, FullFileName) ->
    % prepare data
    fslogic_context:set_context(Context),
    {ok, RemoteLocationDocs} = list_all_available_blocks(ProtocolVersion, CacheName, FileId),
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyRemoteLocationDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id == ProviderId end, RemoteLocationDocs),
    FileId = MyRemoteLocationDoc#db_document.record#available_blocks.file_id,

    % modify document
    NewDoc = fslogic_available_blocks:mark_as_modified(#offset_range{offset = Offset, size = Size}, MyRemoteLocationDoc),

    % notify cache, db and fuses
    case MyRemoteLocationDoc == NewDoc of
        true -> ok;
        false ->
            %update size if this write extends file
            {ok, {Stamp, FileSize}} = get_file_size(ProtocolVersion, CacheName, FileId),
            NewFileSizeTuple = case FileSize < Offset + Size of
                                   true -> {fslogic_available_blocks:get_timestamp(), Offset + Size};
                                   false -> {Stamp, FileSize}
                               end,
            FinalNewDoc = update_size(NewDoc, NewFileSizeTuple),

            %save available_blocks and notify fuses
            save_available_blocks(ProtocolVersion, CacheName, FinalNewDoc, FuseId),

            fslogic_req_regular:update_file_block_map(FullFileName, [{Offset, Size}], false)
    end,
    #atom{value = ?VOK}.

file_block_modified(ProtocolVersion, CacheName, Context, FileId, ?CLUSTER_FUSE_ID, _, Offset, Size, FullFileName, _) ->
    ct:print("file_block_modified_CLUSTER(~p, ~p, ~p, ~p, ~p, ~p, ~p)",[ProtocolVersion, CacheName, Context, FileId, Offset, Size, FullFileName]),
    file_block_modified_internal(ProtocolVersion, CacheName, Context, FileId, ?CLUSTER_FUSE_ID, Offset, Size, FullFileName);

file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout) ->
    ExpectedSequenceNumber = get_expected_sequence_number(CacheName, FileId, FuseId),
    ct:print("file_block_modified(~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p)",[ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout]),
    case SequenceNumber =< ExpectedSequenceNumber of
        true ->
            set_expected_sequence_number(CacheName, FileId, FuseId, SequenceNumber + 1),
            file_block_modified_internal(ProtocolVersion, CacheName, Context, FileId, FuseId, Offset, Size, FullFileName);
        _ ->
            receive
                {file_block_modified, NewContext, FileId, ?CLUSTER_FUSE_ID, _, NewOffset, NewSize, NewFullFileName} ->
                    file_block_modified_internal(ProtocolVersion, CacheName, NewContext, FileId, ?CLUSTER_FUSE_ID, NewOffset, NewSize, NewFullFileName),
                    file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout);
                {file_block_modified, NewContext, FileId, FuseId, ExpectedSequenceNumber, NewOffset, NewSize, NewFullFileName} ->
                    set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                    file_block_modified_internal(ProtocolVersion, CacheName, NewContext, FileId, FuseId, NewOffset, NewSize, NewFullFileName),
                    file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout);
                {file_truncated, NewContext, FileId, ?CLUSTER_FUSE_ID, _, NewSize, NewFullFileName} ->
                    file_truncated_internal(ProtocolVersion, CacheName, NewContext, FileId, ?CLUSTER_FUSE_ID, NewSize, NewFullFileName),
                    file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout);
                {file_truncated, NewContext, FileId, FuseId, ExpectedSequenceNumber, NewSize, NewFullFileName} ->
                    set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                    file_truncated_internal(ProtocolVersion, CacheName, NewContext, FileId, FuseId, NewSize, NewFullFileName),
                    file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, Timeout)
            after Timeout ->
                set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                file_block_modified(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Offset, Size, FullFileName, 0)
            end
    end.

file_truncated_internal(ProtocolVersion, CacheName, Context, FileId, FuseId, Size, FullFileName) ->
    % prepare data
    fslogic_context:set_context(Context),
    {ok, RemoteLocationDocs} = list_all_available_blocks(ProtocolVersion, CacheName, FileId),
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyRemoteLocationDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id == ProviderId end, RemoteLocationDocs),

    % modify document(with size)
    NewDoc_ = fslogic_available_blocks:truncate({bytes, Size}, MyRemoteLocationDoc),
    NewDoc = #db_document{record = #available_blocks{file_parts = Ranges}} = update_size(NewDoc_, {fslogic_available_blocks:get_timestamp(), Size}),

    % notify cache, db and fuses
    case MyRemoteLocationDoc == NewDoc of
        true -> ok;
        false ->
            save_available_blocks(ProtocolVersion, CacheName, NewDoc, FuseId),
            fslogic_req_regular:update_file_block_map(FullFileName, fslogic_available_blocks:ranges_to_offset_tuples(Ranges), true)
    end,
    #atom{value = ?VOK}.

file_truncated(ProtocolVersion, CacheName, Context, FileId, ?CLUSTER_FUSE_ID, _, Size, FullFileName, _) ->
    file_truncated_internal(ProtocolVersion, CacheName, Context, FileId, ?CLUSTER_FUSE_ID, Size, FullFileName);

file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, Timeout) ->
    ExpectedSequenceNumber = get_expected_sequence_number(CacheName, FileId, FuseId),
    case SequenceNumber =< ExpectedSequenceNumber of
        true ->
            set_expected_sequence_number(CacheName, FileId, FuseId, SequenceNumber + 1),
            file_truncated_internal(ProtocolVersion, CacheName, Context, FileId, FuseId, Size, FullFileName);
        _ ->
            receive
                {file_truncated, NewContext, FileId, ?CLUSTER_FUSE_ID, _, NewSize, NewFullFileName} ->
                    file_truncated_internal(ProtocolVersion, CacheName, NewContext, FileId, ?CLUSTER_FUSE_ID, NewSize, NewFullFileName),
                    file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, Timeout);
                {file_truncated, NewContext, FileId, FuseId, ExpectedSequenceNumber, NewSize, NewFullFileName} ->
                    set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                    file_truncated_internal(ProtocolVersion, CacheName, NewContext, FileId, FuseId, NewSize, NewFullFileName),
                    file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, Timeout);
                {file_block_modified, NewContext, FileId, ?CLUSTER_FUSE_ID, _, NewOffset, NewSize, NewFullFileName} ->
                    file_block_modified_internal(ProtocolVersion, CacheName, NewContext, FileId, ?CLUSTER_FUSE_ID, NewOffset, NewSize, NewFullFileName),
                    file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, Timeout);
                {file_block_modified, NewContext, FileId, FuseId, ExpectedSequenceNumber, NewOffset, NewSize, NewFullFileName} ->
                    set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                    file_block_modified_internal(ProtocolVersion, CacheName, NewContext, FileId, FuseId, NewOffset, NewSize, NewFullFileName),
                    file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, Timeout)
            after Timeout ->
                set_expected_sequence_number(CacheName, FileId, FuseId, ExpectedSequenceNumber + 1),
                file_truncated(ProtocolVersion, CacheName, Context, FileId, FuseId, SequenceNumber, Size, FullFileName, 0)
            end
    end.

external_available_blocks_changed(ProtocolVersion, CacheName, Context, FileId, DocumentUuid) ->
    % prepare data
    fslogic_context:set_context(Context),
    MyProviderId = cluster_manager_lib:get_provider_id(),
    {ok, Docs} = dao_lib:apply(dao_vfs, available_blocks_by_file_id, [FileId], 1),
    MyDocs = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id_}}) ->
        Id_ == MyProviderId end, Docs),

    % if my doc exists...
    case MyDocs of
        [MyDoc] ->
            % find changed doc
            [ChangedDoc] = lists:filter(fun(#db_document{uuid = Uuid_}) ->
                utils:ensure_list(Uuid_) == utils:ensure_list(DocumentUuid) end, Docs),

            % modify my doc (with size) according to changed doc
            NewDoc_ = fslogic_available_blocks:mark_other_provider_changes(MyDoc, ChangedDoc),
            NewDoc = #db_document{record = #available_blocks{file_parts = Ranges}} = update_size(NewDoc_, ChangedDoc#db_document.record#available_blocks.file_size), %todo probably don't need to update size every time

            % notify cache, db and fuses
            invalidate_blocks_cache(ProtocolVersion, CacheName, FileId),
            case NewDoc == MyDoc of
                true -> ok;
                _ ->
                    save_available_blocks(ProtocolVersion, CacheName, NewDoc),
                    {ok, FullFileName} = logical_files_manager:get_file_full_name_by_uuid(FileId),
                    {ok, _} = fslogic_req_regular:update_file_block_map(FullFileName, fslogic_available_blocks:ranges_to_offset_tuples(Ranges), true)
            end;
        _ -> ok
    end.

save_available_blocks(ProtocolVersion, CacheName, Doc) ->
    save_available_blocks(ProtocolVersion, CacheName, Doc, undefined).

save_available_blocks(ProtocolVersion, CacheName, Doc, IgnoredFuse) ->
    % save block to db
    {ok, Uuid} = dao_lib:apply(dao_vfs, save_available_blocks, [Doc], ProtocolVersion), % [Doc#db_document{force_update = true}]
    {ok, NewDoc = #db_document{record = #available_blocks{file_id = FileId, file_size = NewDocSize}}} =
        dao_lib:apply(dao_vfs, get_available_blocks, [Uuid], ProtocolVersion),

    % clear cache
    OldSizeQueryResult = clear_size_cache(CacheName, FileId),
    OldDocsQueryResult = clear_docs_cache(CacheName, FileId),

    % create cache again
    case {OldDocsQueryResult, OldSizeQueryResult} of
        {OldDocs, OldSize} when OldDocs =/= undefined andalso OldSize =/= undefined ->
            OtherDocs = [Document || Document = #db_document{uuid = DocId} <- OldDocs, DocId =/= Uuid],
            NewDocs = [NewDoc | OtherDocs],
            NewSize =
                case OldSize of
                    {TS, Val} when TS > element(1, NewDocSize) -> {TS, Val};
                    _ -> NewDocSize
                end,
            update_docs_cache(CacheName, FileId, NewDocs),
            update_size_cache(CacheName, FileId, NewSize, IgnoredFuse);
        _ ->
            {ok, _} = list_all_available_blocks(ProtocolVersion, CacheName, FileId)
    end,
    {ok, Uuid}.

get_available_blocks(ProtocolVersion, _CacheName, FileId) ->
    % list all docs (uses cache)
    {ok, AllDocs} = list_all_available_blocks(ProtocolVersion, _CacheName, FileId),

    % fetch my document
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
        Id == ProviderId end, AllDocs),
    {ok, MyDoc}.

list_all_available_blocks(ProtocolVersion, CacheName, FileId) ->
    % try fetch from cache
    case get_docs_from_cache(CacheName, FileId) of
        Docs when is_list(Docs) -> {ok, Docs};
        undefined ->
            % no cache - fetch all docs from db
            {ok, AllDocs} = dao_lib:apply(dao_vfs, available_blocks_by_file_id, [FileId], ProtocolVersion),

            % create doc for me, if it's not present
            ProviderId = cluster_manager_lib:get_provider_id(),
            CreatedDocs = case lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) ->
                Id == ProviderId end, AllDocs) of
                              [] ->
                                  {ok, Uuid} = dao_lib:apply(dao_vfs, save_available_blocks, [#available_blocks{file_id = FileId, provider_id = ProviderId}], ProtocolVersion),
                                  {ok, Doc} = dao_lib:apply(dao_vfs, get_available_blocks, [Uuid], ProtocolVersion),
                                  [Doc];
                              _ -> []
                          end,

            %add created doc to result
            NewDocs = CreatedDocs ++ AllDocs,

            % find newest size
            Size = lists:foldl(fun(#db_document{record = #available_blocks{file_size = {S, V}}}, {BestS, BestV}) ->
                case S > BestS of true -> {S, V}; _ -> {BestS, BestV} end
            end, {0, 0}, NewDocs),

            % inset results to cache
            update_docs_cache(CacheName, FileId, NewDocs),
            update_size_cache(CacheName, FileId, Size),
            {ok, NewDocs}
    end.

get_file_size(ProtocolVersion, CacheName, FileId) ->
    case get_size_from_cache(CacheName, FileId) of
        undefined ->
            {ok, _} = list_all_available_blocks(ProtocolVersion, CacheName, FileId),
            Size_ = get_size_from_cache(CacheName, FileId),
            {ok, Size_};
        Size ->
            {ok, Size}
    end.

invalidate_blocks_cache(_ProtocolVersion, CacheName, FileId) ->
    clear_docs_cache(CacheName, FileId),
    clear_size_cache(CacheName, FileId).

%% ====================================================================
%% functions for available_blocks documents modification
%% ====================================================================

%% mark_as_modified/2
%% ====================================================================
%% @doc @equiv mark_as_modified(Range, Mydoc, get_timestamp())
%% @end
-spec mark_as_modified(Range :: #byte_range{} | #offset_range{} | #block_range{}, available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_as_modified(Range, #db_document{} = Mydoc) ->
    mark_as_modified(Range, Mydoc, get_timestamp()).

%% mark_as_modified/3
%% ====================================================================
%% @doc Marks given block/byte range as modified.
%% It extends available block list if necessary, and returns updated doc
%% @end
-spec mark_as_modified(Range :: #byte_range{} | #offset_range{} | #block_range{}, available_blocks_doc(), Timestamp :: non_neg_integer()) -> available_blocks_doc().
%% ====================================================================
mark_as_modified(#byte_range{} = ByteRange, #db_document{} = Mydoc, Timestamp) ->
    mark_as_modified(byte_to_block_range(ByteRange), Mydoc, Timestamp);
mark_as_modified(#offset_range{} = OffsetRange, #db_document{} = Mydoc, Timestamp) ->
    mark_as_modified(offset_to_block_range(OffsetRange), Mydoc, Timestamp);
mark_as_modified(#block_range{from = From, to = To}, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc, Timestamp) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:merge(Parts, [#range{from = From, to = To, timestamp = Timestamp}])),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% truncate/2
%% ====================================================================
%% @doc @equiv truncate(Size, MyDoc, get_timestamp())
%% @end
-spec truncate(Range :: {bytes, integer()} | integer(), MyDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
truncate(Size, #db_document{} = MyDoc) ->
    truncate(Size, MyDoc, get_timestamp()).

%% truncate/3
%% ====================================================================
%% @doc Truncates given list of ranges to given size
%% It extends available block list if necessary and returns updated document.
%% @end
-spec truncate(Range :: {bytes, integer()} | integer(), MyDoc :: available_blocks_doc(), Timestamp :: non_neg_integer()) -> available_blocks_doc().
%% ====================================================================
truncate({bytes, ByteSize}, #db_document{} = MyDoc, Timestamp) ->
    truncate(byte_to_block(ByteSize), MyDoc, Timestamp);
truncate(BlockSize, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc, Timestamp) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:truncate(Parts, #range{to = BlockSize - 1, timestamp = Timestamp})),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% mark_as_available/3
%% ====================================================================
%% @doc Marks given block range as available for provider.
%% It extends available block list if necessary, and returns updated document.
%% @end
-spec mark_as_available(Blocks :: [#range{}], MyDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_as_available(Blocks, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:merge(Parts, Blocks)),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% check_if_synchronized/3
%% ====================================================================
%% @doc Checks if given range of bytes/blocks is in sync with other providers.
%% If so, the empty list is returned. If not, function returns list of unsynchronized parts.
%% Each range contains information about providers that have it up-to-date.
%% @end
-spec check_if_synchronized(Range :: #byte_range{} | #offset_range{} | #block_range{}, MyDoc :: available_blocks_doc(), OtherDocs :: [available_blocks_doc()]) ->
    [{ProviderId :: string(), AvailableBlocks :: [#range{}]}].
%% ====================================================================
check_if_synchronized(#byte_range{} = ByteRange, MyDoc, OtherDocs) ->
    check_if_synchronized(byte_to_block_range(ByteRange), MyDoc, OtherDocs);
check_if_synchronized(#offset_range{} = OffsetRange, MyDoc, OtherDocs) ->
    check_if_synchronized(offset_to_block_range(OffsetRange), MyDoc, OtherDocs);
check_if_synchronized(#block_range{from = From, to = To}, #db_document{record = #available_blocks{file_parts = Parts}}, OtherDocs) ->
    PartsOutOfSync = ranges_struct:minimize(ranges_struct:subtract([#range{from = From, to = To}], Parts)),
    lists:map(
        fun(#db_document{record = #available_blocks{file_parts = Parts_, provider_id = Id}}) ->
            {Id, ranges_struct:minimize(ranges_struct:intersection(PartsOutOfSync, Parts_))}
        end, OtherDocs).

%% mark_other_provider_changes/2
%% ====================================================================
%% @doc Deletes blocks changed by other provider, from local available_blocks map
%% @end
-spec mark_other_provider_changes(MyDoc :: available_blocks_doc(), OtherDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_other_provider_changes(MyDoc = #db_document{record = #available_blocks{file_parts = MyParts} = Location}, #db_document{record = #available_blocks{file_parts = OtherParts}}) ->
    NewParts = ranges_struct:minimize(ranges_struct:subtract_newer(MyParts, OtherParts)),
    MyDoc#db_document{record = Location#available_blocks{file_parts = NewParts}}.

update_size(MyDoc = #db_document{record = #available_blocks{file_size = {_, MySizeValue}}}, {_, OtherSizeValue}) when MySizeValue == OtherSizeValue ->
    MyDoc;
update_size(MyDoc = #db_document{record = #available_blocks{file_size = {MyStamp, _}}}, {OtherStamp, _}) when MyStamp >= OtherStamp ->
    MyDoc;
update_size(MyDoc = #db_document{record = AvailableBlocks}, OtherSize) ->
    MyDoc#db_document{record = AvailableBlocks#available_blocks{file_size = OtherSize}}.

%% ====================================================================
%% Utility functions
%% ====================================================================

ranges_to_offset_tuples([]) -> [];
ranges_to_offset_tuples([#range{} = H | T]) ->
    #offset_range{offset = Offset, size = Size} = byte_to_offset_range(block_to_byte_range(H)),
    [{Offset, Size} | ranges_to_offset_tuples(T)].

%% ====================================================================
%% Internal functions
%% ====================================================================

block_to_byte_range(#range{from = From, to = To}) ->
    block_to_byte_range(#block_range{from = From, to = To});
block_to_byte_range(#block_range{from = From, to = To}) ->
    #byte_range{from = From * ?remote_block_size, to = (To + 1) * ?remote_block_size}.

%% block_to_byte_range/2
%% ====================================================================
%% @doc Converts block range to byte range, according to 'remote_block_size'
%% @end
-spec block_to_byte_range(#block_range{}, FileByteSize :: integer()) -> #byte_range{}.
%% ====================================================================
block_to_byte_range(#range{from = From, to = To}, FileByteSize) ->
    block_to_byte_range(#block_range{from = From, to = To}, FileByteSize);
block_to_byte_range(#block_range{from = From, to = To}, FileByteSize) when is_integer(FileByteSize) ->
    #byte_range{from = From * ?remote_block_size, to = min((To + 1) * ?remote_block_size, FileByteSize - 1)}.

%% byte_to_block_range/1
%% ====================================================================
%% @doc Converts byte range to block range, according to 'remote_block_size'
%% @end
-spec byte_to_block_range(#byte_range{}) -> #block_range{}.
%% ====================================================================
byte_to_block_range(#byte_range{from = From, to = To}) ->
    #block_range{from = From div ?remote_block_size, to = To div ?remote_block_size}.

byte_to_offset_range(#byte_range{from = From, to = To}) ->
    #offset_range{offset = From, size = To - From + 1}.

%% offset_to_block_range/1
%% ====================================================================
%% @doc Converts offset to block range, according to 'remote_block_size'
%% @end
-spec offset_to_block_range(#offset_range{}) -> #block_range{}.
%% ====================================================================
offset_to_block_range(#offset_range{offset = Offset, size = Size}) ->
    byte_to_block_range(#byte_range{from = Offset, to = Offset + Size - 1}).

%% byte_to_block/1
%% ====================================================================
%% @doc Converts bytes to blocks
%% @end
-spec byte_to_block(integer()) -> integer().
%% ====================================================================
byte_to_block(Byte) ->
    utils:ceil(Byte / ?remote_block_size).

%% get_timestamp/0
%% ====================================================================
%% @doc gets a timestamp in ms from the epoch
%% @end
-spec get_timestamp() -> non_neg_integer().
%% ====================================================================
get_timestamp() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

%Internal ets cache wrapping functions

get_size_from_cache(CacheName, FileId) ->
    case ets:lookup(CacheName, {FileId, file_size}) of
        [{_, Size = {_, _}}] -> Size;
        [] -> undefined
    end.

get_docs_from_cache(CacheName, FileId) ->
    case ets:lookup(CacheName, {FileId, all_docs}) of
        [{_, Docs}] when is_list(Docs) -> Docs;
        [] -> undefined
    end.

update_size_cache(CacheName, FileId, NewSizeTuple) ->
    update_size_cache(CacheName, FileId, NewSizeTuple, undefined).

update_size_cache(CacheName, FileId, {_, NewSize} = NewSizeTuple, IgnoredFuse) ->
    case ets:lookup(CacheName, {FileId, old_file_size}) of
        [] ->
            ets:delete(CacheName, {FileId, old_file_size}),
            ets:insert(CacheName, {{FileId, file_size}, NewSizeTuple}),
            fslogic_events:on_file_size_update(utils:ensure_list(FileId), 0, NewSize, IgnoredFuse);
        [{_, {_, OldSize}}] when OldSize =/= NewSize ->
            ets:delete(CacheName, {FileId, old_file_size}),
            ets:insert(CacheName, {{FileId, file_size}, NewSizeTuple}),
            fslogic_events:on_file_size_update(utils:ensure_list(FileId), OldSize, NewSize, IgnoredFuse);
        [_] ->
            ets:delete(CacheName, {FileId, old_file_size}),
            ets:insert(CacheName, {{FileId, file_size}, NewSizeTuple})
    end.

update_docs_cache(CacheName, FileId, Docs) when is_list(Docs) ->
    ets:insert(CacheName, {{FileId, all_docs}, Docs}).

clear_size_cache(CacheName, FileId) ->
    case ets:lookup(CacheName, {FileId, file_size}) of
        [{_, OldSize = {_, _}}] ->
            ets:insert(CacheName, {{FileId, old_file_size}, OldSize}),
            ets:delete(CacheName, {FileId, file_size}),
            OldSize;
        [] ->
            ets:delete(CacheName, {FileId, old_file_size}),
            ets:delete(CacheName, {FileId, file_size}),
            undefined
    end.

clear_docs_cache(CacheName, FileId) ->
    case ets:lookup(CacheName, {FileId, all_docs}) of
        [{_, OldDocs}] when is_list(OldDocs) ->
            ets:delete(CacheName, {FileId, all_docs}),
            OldDocs;
        [] ->
            ets:delete(CacheName, {FileId, all_docs}),
            undefined
    end.

get_expected_sequence_number(CacheName, FileId, FuseId) ->
    case ets:lookup(CacheName, {FileId, FuseId, sequence_number}) of
        [] -> 0;
        [{{FileId, FuseId, sequence_number}, SequenceNumber}] -> SequenceNumber
    end.

set_expected_sequence_number(CacheName, FileId, FuseId, SequenceNumber) ->
    ets:insert(CacheName, {{FileId, FuseId, sequence_number}, SequenceNumber}).
