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
-define(ARCHIVE_PRESERVED, preserved).
-define(ARCHIVE_FAILED, failed).
-define(ARCHIVE_PURGING, purging).


% Macros defining possible layouts of files and directories in an archive
-define(ARCHIVE_PLAIN_LAYOUT, plain).
-define(ARCHIVE_BAGIT_LAYOUT, bagit).
% TODO VFS-7651 add ?ARCHIVE_BAGIT_LAYOUT to the following list
-define(ARCHIVE_LAYOUTS, [?ARCHIVE_PLAIN_LAYOUT]).

% TODO VFS-7652 add true to the below list
-define(SUPPORTED_INCREMENTAL_VALUES, [false]).
% TODO VFS-7653 add true to the below list
-define(SUPPORTED_INCLUDE_DIP_VALUES, [false]).

-define(DEFAULT_LAYOUT, ?ARCHIVE_PLAIN_LAYOUT).
-define(DEFAULT_INCLUDE_DIP, false).
-define(DEFAULT_INCREMENTAL, false).
-define(DEFAULT_ARCHIVE_DESCRIPTION, <<>>).

-record(archive_config, {
    incremental = ?DEFAULT_INCREMENTAL :: archive_config:incremental(),
    % This flag determines whether dissemination information package (DIP) is created alongside with
    % archival information package (AIP), on the storage.
    include_dip = ?DEFAULT_INCLUDE_DIP :: archive_config:include_dip(),
    layout :: archive_config:layout()
}).

-endif.