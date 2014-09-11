%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mapping of rest subpaths to erlang modules
%% that will end up handling REST requests.
%% @end
%% ===================================================================

-module(rest_routes).

-include("veil_modules/control_panel/global_registry_interfacing.hrl").

-export([route/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% route/1
%% ====================================================================
%% @doc 
%% This function returns handler module and resource ID based on REST request path.
%% The argument is a list of binaries - result of splitting request subpath on "/".
%% Subpath is all that occurs after ``"<host>/rest/<version>/"'' in request path.
%% Should return a tuple:
%% - the module that will be called to handle requested REST resource (atom)
%% - resource id or undefined if none was specified (binary or atom (undefined))
%% or {error, path_invalid} if no module was matched
%% @end
-spec route([binary()]) -> {atom(), binary() | undefined} | {error, path_invalid} .
%% ====================================================================
route([<<"files">>])                -> {rest_files, undefined};
route([<<"files">>|Path])           -> {rest_files, rest_utils:join_to_path(Path)};
route([<<"attrs">>])                -> {rest_attrs, undefined};
route([<<"attrs">>|Path])           -> {rest_attrs, rest_utils:join_to_path(Path)};
route([<<"shares">>])               -> {rest_shares, undefined};
route([<<"shares">>, ID])           -> {rest_shares, ID};
route([?connection_check_path])     -> {rest_connection_check, undefined};
route(_)                            -> {error, path_invalid}.

