%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Type definitions for fslogic
%% @end
%% ===================================================================
-author("Rafal Slota").

-ifndef(FSLOGIC_TYPES_HRL).
-define(FSLOGIC_TYPES_HRL, 1).

-type fslogic_error() :: string() | atom(). %% Note: Only values from ?ALL_ERROR_CODES are allowed for this type.

-endif.