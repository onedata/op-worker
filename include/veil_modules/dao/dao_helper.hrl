%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_helper header
%% @end
%% ===================================================================

-include_lib("veil_modules/dao/couch_db.hrl").

-record(view_row, {id = "", key = "", value = 0}).
-record(view_result, {total = 0, offset = 0, rows = []}).