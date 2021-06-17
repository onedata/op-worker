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

-define(MD5, md5).
-define(SHA1, sha1).
-define(SHA256, sha256).
-define(SHA512, sha512).

-define(SUPPORTED_CHECKSUM_ALGORITHMS, [?MD5, ?SHA1, ?SHA256, ?SHA512]).

-define(CHECKSUM_MANIFEST_FILE_NAME_FORMAT, "manifest-~s.txt").
-define(CHECKSUM_MANIFEST_FILE_NAME(Algorithm),
    str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_NAME_FORMAT, [ChecksumAlgorithm])).


-endif.