%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros used in archivisation_tree module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ARCHIVISATION_TREE_HRL).
-define(ARCHIVISATION_TREE_HRL, 1).

% Macros associated with archivisation
-define(ARCHIVES_ROOT_DIR_UUID_PREFIX, "archives_root_").
-define(ARCHIVES_ROOT_DIR_UUID(SpaceId), <<?ARCHIVES_ROOT_DIR_UUID_PREFIX, SpaceId/binary>>).
-define(ARCHIVES_ROOT_DIR_NAME, file_meta:hidden_file_name(<<"archive">>)).

-define(DATASET_ARCHIVES_DIR_UUID_PREFIX, "dataset_archives_").
-define(DATASET_ARCHIVES_DIR_UUID(DatasetId), <<?DATASET_ARCHIVES_DIR_UUID_PREFIX, DatasetId/binary>>).

-define(ARCHIVE_DIR_UUID_PREFIX, "archive_").
-define(ARCHIVE_DIR_UUID(ArchiveId), <<?ARCHIVE_DIR_UUID_PREFIX, ArchiveId/binary>>).

-define(ARCHIVES_ROOT_DIR_PERMS, 8#755).
-define(DATASET_ARCHIVES_DIR_PERMS, 8#755).

-endif.