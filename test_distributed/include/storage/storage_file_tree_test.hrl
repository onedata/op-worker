%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Declarations of file tree nodes that exist only on a storage backend, i.e.
%%% ones with no counterpart in the generic (logical) file tree vocabulary of
%%% file/file_tree_test.hrl. Consumed by storage_file_tree_test_utils, which
%%% builds such trees directly on the storage.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_FILE_TREE_TEST_HRL).
-define(STORAGE_FILE_TREE_TEST_HRL, 1).

-include("file/file_tree_test.hrl").


%% Declares a FIFO (named pipe) to be created directly on the storage. A FIFO is a
%% special (unsupported) file type that storage import must ignore - it is created
%% on the storage but never imported into the logical filesystem.
-record(storage_fifo_spec, {
    name = undefined :: undefined | binary()
}).


-endif.
