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

-include("files_common.hrl").
-include("oneprovider_modules/dao/dao_spaces.hrl").

-type fslogic_error() :: string() | atom().     %% Values: ?ALL_ERROR_CODES are allowed for this type.
-type file_type() :: ?DIR_TYPE | ?REG_TYPE | ?LNK_TYPE.
-type file_type_protocol() :: string().         %% Values: ?DIR_TYPE_PROT, ?REG_TYPE_PROT, ?LNK_TYPE_PROT

-type space_info() :: #space_info{}.

-type storage_file_id() :: string().

-endif.