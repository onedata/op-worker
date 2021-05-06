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
-define(FULL_ARCHIVE, full).
-define(INCREMENTAL_ARCHIVE, incremental).
-define(ARCHIVE_TYPES, [?FULL_ARCHIVE, ?INCREMENTAL_ARCHIVE]).

% Macros defining possible states of archive
-define(EMPTY, empty).
-define(INITIALIZING, initializing).
-define(ARCHIVED, archived).
-define(FAILED, failed).
-define(PURGING, purging).

% Macros defining possible characters of an archive
-define(DIP, dip).
-define(AIP, aip).
-define(HYBRID, hybrid).
-define(ARCHIVE_CHARACTERS, [?DIP, ?AIP, ?HYBRID]).

% Macros defining possible archive data structure types
-define(BAGIT, bagit).
-define(SIMPLE_COPY, simpleCopy).
-define(ARCHIVE_DATA_STRUCTURES, [?BAGIT, ?SIMPLE_COPY]).

% Macros defining possible archive metadata structure types
-define(BUILT_IN, builtIn).
-define(JSON, json).
-define(XML, xml).
-define(ARCHIVE_METADATA_STRUCTURES, [?BUILT_IN, ?JSON, ?XML]).


-record(archive_params, {
    type :: archive_params:type(),
    character :: archive_params:character(),
    data_structure :: archive_params:data_structure(),
    metadata_structure :: archive_params:metadata_structure()
}).

-record(archive_attrs, {
    % TODO VFS-7616 add more attrs?
    description :: archive_attrs:description() | undefined
}).


-endif.