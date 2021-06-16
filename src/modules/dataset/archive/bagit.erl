%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME napisać jakie RFC i dać linka
%%% @end
%%%-------------------------------------------------------------------
-module(bagit).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([prepare_bag/2, add_checksum_entry/5]).

-define(DATA_DIR_NAME, <<"data">>).

-define(BAG_DECLARATION_FILE_NAME, <<"bagit.txt">>).
-define(VERSION, "1.0").
-define(ENCODING, "UTF-8").

-define(CHECKSUM_MANIFEST_FILE_NAME_FORMAT, "manifest-~s.txt").
-define(CHECKSUM_MANIFEST_FILE_ENTRY_FORMAT, "~s    ~s~n"). % <FILEPATH>    <CHECKSUM_VALUE>\n

-type checksum_algorithm() :: md5 | sha1 | sha256 | sha512.

% TODO optional elements - porobić tickety
% TODO tagmanifest-<algorithm>.txt
% TODO bag-info.txt


%%%===================================================================
%%% API functions
%%%===================================================================

-spec prepare_bag(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
prepare_bag(ParentCtx, UserCtx) ->
    DataDirCtx = create_data_dir(ParentCtx, UserCtx),
    create_bag_declaration(ParentCtx, UserCtx),
    ChecksumAlgorithms = [md5 , sha1 , sha256 , sha512], % todo allow to pass this algorithms in archivisation request as param
    create_checksum_algorithms_manifests(ParentCtx, UserCtx, ChecksumAlgorithms),
    {ok, DataDirCtx}.


-spec add_checksum_entry(file_ctx:ctx(), user_ctx:ctx(), file_meta:path(), binary(), checksum_algorithm()) -> ok.
add_checksum_entry(ParentCtx, UserCtx, FilePath, Checksum, ChecksumAlgorithm) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ManifestFileName = str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_NAME_FORMAT, [ChecksumAlgorithm]),
    {ManifestFileCtx, _} = files_tree:get_child(ParentCtx, ManifestFileName, UserCtx),
    ManifestFileGuid = file_ctx:get_logical_guid_const(ManifestFileCtx),
    {ok, Handle} = lfm:open(SessionId, ?FILE_REF(ManifestFileGuid), write),
    Entry = str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_ENTRY_FORMAT, [FilePath, Checksum]),
    {ok, _, _} = lfm:write(Handle, 0, Entry),
    lfm:release(Handle).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_data_dir(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
create_data_dir(ParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, DataDirGuid} = lfm:mkdir(SessionId, ParentGuid, ?DATA_DIR_NAME, ?DEFAULT_DIR_MODE),
    file_ctx:new_by_guid(DataDirGuid).


-spec create_bag_declaration(file_ctx:ctx(), user_ctx:ctx()) -> ok.
create_bag_declaration(ParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, {_Guid, Handle}} = lfm:create_and_open(SessionId, ParentGuid, ?BAG_DECLARATION_FILE_NAME, ?DEFAULT_FILE_MODE, write),

    Content = str_utils:format_bin(
        "BagIt-Version: ~s~n"
        "Tag-File-Character-Encoding: ~s", [?VERSION, ?ENCODING]
    ),
    {ok, _, _} = lfm:write(Handle, 0, Content),
    lfm:release(Handle).


-spec create_checksum_algorithms_manifests(file_ctx:ctx(), user_ctx:ctx(), [checksum_algorithm()]) -> ok.
create_checksum_algorithms_manifests(ParentCtx, UserCtx, ChecksumAlgorithms) ->
    lists:foreach(fun(ChecksumAlgorithm) ->
        create_checksum_algorithm_manifest(ParentCtx, UserCtx, ChecksumAlgorithm)
    end, ChecksumAlgorithms).


-spec create_checksum_algorithm_manifest(file_ctx:ctx(), user_ctx:ctx(), checksum_algorithm()) -> ok.
create_checksum_algorithm_manifest(ParentCtx, UserCtx, ChecksumAlgorithm) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    ManifestFileName = str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_NAME_FORMAT, [ChecksumAlgorithm]),
    {ok, _} = lfm:create(SessionId, ParentGuid, ManifestFileName, ?DEFAULT_FILE_MODE),
    ok.

