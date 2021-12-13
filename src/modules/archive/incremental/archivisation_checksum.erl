%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for calculating and storing checksums of archived 
%%% files. It is used to calculate and save (as xattr) archived file's
%%% checksum so that it can be compared when creating incremental archive 
%%% or validating newly created one.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_checksum).
-author("Jakub Kudzia").

-include("modules/logical_file_manager/utils/file_checksum.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").

-type checksum_type() :: content | metadata | children_count.

%% API
-export([file_calculate_and_save/2, dir_calculate_and_save/3]).
-export([has_file_changed/3, has_dir_changed/4]).

-define(ALGORITHM, ?MD5).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec file_calculate_and_save(file_ctx:ctx(), user_ctx:ctx()) -> ok.
file_calculate_and_save(ArchivedFileCtx, UserCtx) ->
    ok = save(ArchivedFileCtx, calculate(ArchivedFileCtx, UserCtx, content), content),
    ok = save(ArchivedFileCtx, calculate(ArchivedFileCtx, UserCtx, metadata), metadata).


-spec dir_calculate_and_save(file_ctx:ctx(), user_ctx:ctx(), non_neg_integer()) -> ok.
dir_calculate_and_save(ArchivedDirCtx, UserCtx, ChildrenCount) ->
    ok = save(ArchivedDirCtx, calculate(ArchivedDirCtx, UserCtx, metadata), metadata),
    ok = save(ArchivedDirCtx, ChildrenCount, children_count).


has_file_changed(ArchivedFileCtx, FileCtx, UserCtx) ->
    has_checksum_changed(ArchivedFileCtx, FileCtx, UserCtx, metadata) orelse
        has_checksum_changed(ArchivedFileCtx, FileCtx, UserCtx, content).


has_dir_changed(ArchivedDirCtx, DirCtx, UserCtx, CurrentChildrenCount) ->
    has_checksum_changed(ArchivedDirCtx, DirCtx, UserCtx, metadata) orelse
        get(ArchivedDirCtx, children_count) =/= CurrentChildrenCount.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec save(file_ctx:ctx(), file_checksum:checksum() | non_neg_integer(), checksum_type()) -> ok.
save(ArchivedFileCtx, Checksum, Type) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    ?FUSE_OK_RESP = xattr_req:set_xattr(RootUserCtx, ArchivedFileCtx, #xattr{
        name = type_to_checksum_key(Type),
        value = Checksum
    }, true, false),
    ok.


-spec get(file_ctx:ctx(), checksum_type()) -> file_checksum:checksum() | non_neg_integer().
get(ArchivedFileCtx, Type) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    case xattr_req:get_xattr(RootUserCtx, ArchivedFileCtx, type_to_checksum_key(Type), false) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = #xattr{value = Checksum}} -> 
            Checksum;
        _ ->
            <<>>
    end.


-spec calculate(file_ctx:ctx(), user_ctx:ctx(), checksum_type()) -> file_checksum:checksum().
calculate(FileCtx, UserCtx, content) ->
    Checksums = file_checksum:calculate(FileCtx, UserCtx, ?ALGORITHM),
    file_checksum:get(?ALGORITHM, Checksums);
calculate(FileCtx, UserCtx, metadata) ->
    % currently only json metadata are archived
    Metadata = get_json_metadata(FileCtx, UserCtx),
    HashState = crypto:hash_init(?ALGORITHM),
    FinalHashState = crypto:hash_update(HashState, json_utils:encode(Metadata)),
    hex_utils:hex(crypto:hash_final(FinalHashState)).


-spec has_checksum_changed(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx(), checksum_type()) -> 
    boolean().
has_checksum_changed(ArchivedFileCtx, FileCtx, UserCtx, Type) ->
    ArchivedFileChecksum = get(ArchivedFileCtx, Type),
    FileChecksum = calculate(FileCtx, UserCtx, Type),
    ArchivedFileChecksum =/= FileChecksum.


-spec get_json_metadata(file_ctx:ctx(), user_ctx:ctx()) -> json_utils:json_term() | undefined.
get_json_metadata(FileCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    case lfm:get_metadata(SessionId, ?FILE_REF(FileGuid), json, [], false) of
        {ok, JsonMetadata} -> JsonMetadata;
        {error, ?ENODATA} -> undefined
    end.


-spec type_to_checksum_key(checksum_type()) -> binary().
type_to_checksum_key(content) -> ?ARCHIVISATION_CONTENT_CHECKSUM_KEY;
type_to_checksum_key(metadata) -> ?ARCHIVISATION_METADATA_CHECKSUM_KEY;
type_to_checksum_key(children_count) -> ?ARCHIVISATION_CHILDREN_COUNT_KEY.
