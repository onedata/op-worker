%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros used in fslogic_delete and file_handles modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FSLOGIC_DELETE_HRL).
-define(FSLOGIC_DELETE_HRL, 1).

%% Macros defining removal_status used in file_handles document.
-define(NOT_REMOVED, not_removed).
-define(LOCAL_REMOVE, local_remove).
-define(REMOTE_REMOVE, remote_remove).

% Macros defining methods of handling opened files
-define(RENAME_HANDLING_METHOD, rename).
-define(MARKER_HANDLING_METHOD, deletion_marker).


-endif.