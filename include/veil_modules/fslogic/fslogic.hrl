%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: fslogic main header
%% @end
%% ===================================================================


%% POSIX error names
-define(VOK,        "ok").       %% Everything is just great
-define(VENOENT,    "enoent").   %% File not found
-define(VEACCES,    "eacces").   %% User doesn't have access to requested resource (e.g. file)
-define(VEEXIST,    "eexist").   %% Given file already exist
-define(VEIO,       "eio").      %% Input/output error - default error code for unknown errors
-define(VENOTSUP, 	"enotsup").  %% Operation not supported
-define(VENOTEMPTY,  "enotempty").%% Directory is not empty