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

% Macros defining possible states of archive
-define(EMPTY, empty).
-define(INITIALIZING, initializing).
-define(ARCHIVED, archived).
-define(RETIRING, retiring).

% Macros defining possible characters of an archive
-define(DIP, dip).
-define(AIP, aip).
-define(DIP_AIP, dip_aip).

% Macros defining possible archive data structure types
-define(BAGIT, bagit).
-define(SIMPLE_COPY, simple_copy).

% Macros defining possible archive metadata structure types
-define(BUILT_IN, built_in).
-define(JSON, json).
-define(XML, xml).

-endif.