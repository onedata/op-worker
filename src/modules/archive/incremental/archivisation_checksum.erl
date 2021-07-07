%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for incremental_archive.
%%% It is used to calculate and save (as xattr) archived file's
%%% checksum so that it can be compared when creating next archive.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_checksum).
-author("Jakub Kudzia").

-include("modules/logical_file_manager/utils/file_checksum.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").


%% API
-export([calculate/2, calculate_and_save/2, get/1, save/2]).

-define(ALGORITHM, ?MD5).
-define(ARCHIVISATION_METADATA_KEY,
    str_utils:join_binary([?ONEDATA_PREFIX, <<"archivisation.checksum.md5">>])).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec calculate(file_ctx:ctx(), user_ctx:ctx()) -> file_checksum:checksum().
calculate(FileCtx, UserCtx) ->
    Checksums = file_checksum:calculate(FileCtx, UserCtx, ?ALGORITHM),
    file_checksum:get(?ALGORITHM, Checksums).


-spec calculate_and_save(file_ctx:ctx(), user_ctx:ctx()) -> ok.
calculate_and_save(ArchivedFileCtx, UserCtx) ->
    save(ArchivedFileCtx, calculate(ArchivedFileCtx, UserCtx)).


-spec save(file_ctx:ctx(), file_checksum:checksum()) -> ok.
save(ArchivedFileCtx, Checksum) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    ?FUSE_OK_RESP = xattr_req:set_xattr(RootUserCtx, ArchivedFileCtx, #xattr{
        name = ?ARCHIVISATION_METADATA_KEY,
        value = Checksum
    }, true, false),
    ok.


-spec get(file_ctx:ctx()) -> file_checksum:checksum().
get(ArchivedFileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #xattr{value = Checksum}
    } = xattr_req:get_xattr(RootUserCtx, ArchivedFileCtx, ?ARCHIVISATION_METADATA_KEY, false),
    Checksum.