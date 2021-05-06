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

% Macros defining possible types of archives
% ?FULL_ARCHIVE is an archive in which
% all files belonging to the archived dataset are stored in the archive.
-define(FULL_ARCHIVE, full).
% ?INCREMENTAL_ARCHIVE is an archive in which
% only files which has changed since previous version of an archive are stored.
% Unchanged files are hardlinks to files in the previous version.
-define(INCREMENTAL_ARCHIVE, incremental).
-define(ARCHIVE_TYPES, [?FULL_ARCHIVE, ?INCREMENTAL_ARCHIVE]).

% Macros defining possible states of archive
-define(EMPTY, empty).
-define(INITIALIZING, initializing).
-define(PERSISTED, persisted).
-define(FAILED, failed).
-define(PURGING, purging).

% Macros defining possible archive data structure types
-define(BAGIT, bagit).
-define(SIMPLE_COPY, simpleCopy).
-define(ARCHIVE_DATA_STRUCTURES, [?BAGIT, ?SIMPLE_COPY]).

% Macros defining possible archive metadata structure types
-define(BUILT_IN, builtIn).
-define(JSON, json).
-define(XML, xml).
-define(ARCHIVE_METADATA_STRUCTURES, [?BUILT_IN, ?JSON, ?XML]).

-define(DEFAULT_DIP, false).

-record(archive_params, {
    type :: archive_params:type(),
    % This flag determines whether dissemination information package (DIP) is created alongside with
    % archival information package (AIP), on the storage.
    dip = ?DEFAULT_DIP :: archive_params:dip(),
    data_structure :: archive_params:data_structure(),
    metadata_structure :: archive_params:metadata_structure(),
    callback :: archive_params:url_callback() | undefined
}).

-record(archive_attrs, {
    % TODO VFS-7616 add more attrs?
    description :: archive_attrs:description() | undefined
}).


-endif.