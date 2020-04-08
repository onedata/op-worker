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

%% Macros defining modes of file deletions
-define(NOT_DELETED, not_deleted).
-define(LOCAL_DELETE, local_delete).
-define(REMOTE_DELETE, remote_delete).
-define(OPENED_FILE_DELETE, opened_file_delete).
-define(RELEASED_FILE_DELETE(LocalOrRemote), {released_file_delete, LocalOrRemote}).

-endif.