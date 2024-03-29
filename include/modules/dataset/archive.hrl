%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in modules that implement archives functionality.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ARCHIVE_HRL).
-define(ARCHIVE_HRL, 1).

% TODO VFS-7616 refine archives' attributes, describe and explain following macros

% Macros defining possible states of an archive
-define(ARCHIVE_PENDING, pending).
-define(ARCHIVE_BUILDING, building).
-define(ARCHIVE_VERIFYING, verifying).
-define(ARCHIVE_PRESERVED, preserved).
-define(ARCHIVE_FAILED, failed).
-define(ARCHIVE_VERIFICATION_FAILED, verification_failed).
-define(ARCHIVE_DELETING, deleting).
-define(ARCHIVE_CANCELLING(PreservationPolicy), {cancelling, PreservationPolicy}).
-define(ARCHIVE_CANCELLED, cancelled).


% Macros defining possible layouts of files and directories in an archive
-define(ARCHIVE_PLAIN_LAYOUT, plain).
-define(ARCHIVE_BAGIT_LAYOUT, bagit).
-define(ARCHIVE_LAYOUTS, [?ARCHIVE_PLAIN_LAYOUT, ?ARCHIVE_BAGIT_LAYOUT]).

-define(SUPPORTED_INCREMENTAL_ENABLED_VALUES, [false, true]).
-define(SUPPORTED_INCLUDE_DIP_VALUES, [true, false]).

-define(DEFAULT_LAYOUT, ?ARCHIVE_PLAIN_LAYOUT).
-define(DEFAULT_INCLUDE_DIP, false).
-define(DEFAULT_INCREMENTAL, #{<<"enabled">> => false}).
-define(DEFAULT_BASE_ARCHIVE, null).
-define(DEFAULT_ARCHIVE_DESCRIPTION, <<>>).
-define(DEFAULT_CREATE_NESTED_ARCHIVES, false).
-define(DEFAULT_ARCHIVE_FOLLOW_SYMLINKS, true).

-record(archive_config, {
    incremental = ?DEFAULT_INCREMENTAL :: archive_config:incremental(),
    % This flag determines whether dissemination information package (DIP) is created alongside with
    % archival information package (AIP), on the storage.
    include_dip = ?DEFAULT_INCLUDE_DIP :: archive_config:include_dip(),
    layout :: archive_config:layout(),
    % this flag determines whether archives of nested datasets should be created
    % during archivisation of a dataset
    create_nested_archives = ?DEFAULT_CREATE_NESTED_ARCHIVES :: boolean(),
    follow_symlinks = ?DEFAULT_ARCHIVE_FOLLOW_SYMLINKS :: boolean()
}).

-record(archive_stats, {
    files_archived = 0 :: non_neg_integer(),
    files_failed = 0 :: non_neg_integer(),
    bytes_archived = 0 :: non_neg_integer()
}).

-endif.