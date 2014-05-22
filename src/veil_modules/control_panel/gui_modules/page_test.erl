%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page used by globalregistry to test connection.
%% @end
%% ===================================================================

-module(page_test).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> <<"gui">>.
