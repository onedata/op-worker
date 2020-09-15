%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros associated with auto_imported_spaces_registry.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(AUTO_IMPORTED_SPACES_REGISTRY_HRL).
-define(AUTO_IMPORTED_SPACES_REGISTRY_HRL, 1).

% status of registry
-define(INITIALIZED, initialized).
-define(NOT_INITIALIZED, not_initialized).

% status of storage import in the registered space
-define(SCANNING, scanning).
-define(INACTIVE, inactive).

-endif.