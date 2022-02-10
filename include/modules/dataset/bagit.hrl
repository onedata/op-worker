%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in modules associated with bagit archives.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BAGIT_HRL).
-define(BAGIT_HRL, 1).

-include("modules/logical_file_manager/utils/file_checksum.hrl").

-define(SUPPORTED_CHECKSUM_ALGORITHMS, [?MD5, ?SHA1, ?SHA256, ?SHA512]).

-define(CHECKSUM_MANIFEST_FILE_NAME_FORMAT, "manifest-~s.txt").
-define(CHECKSUM_MANIFEST_FILE_NAME(Algorithm),
    str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_NAME_FORMAT, [Algorithm])).

-define(TAG_MANIFEST_FILE_NAME_FORMAT, "tagmanifest-~s.txt").
-define(TAG_MANIFEST_FILE_NAME(Algorithm),
    str_utils:format_bin(?TAG_MANIFEST_FILE_NAME_FORMAT, [Algorithm])).

-define(BAGIT_DATA_DIR_NAME, <<"data">>).

-define(BAG_DECLARATION_FILE_NAME, <<"bagit.txt">>).
-define(VERSION, "1.0").
-define(ENCODING, "UTF-8").

-define(MANIFEST_FILE_ENTRY_FORMAT, "~s    ~s~n"). % <CHECKSUM_VALUE>    <FILEPATH>\n
-define(MANIFEST_FILE_ENTRY(Checksum, FilePath),
    str_utils:format_bin(?MANIFEST_FILE_ENTRY_FORMAT, [Checksum, FilePath])).

-define(METADATA_FILE_NAME, <<"metadata.json">>).


-endif.