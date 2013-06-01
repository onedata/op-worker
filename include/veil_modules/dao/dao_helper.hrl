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

-ifndef(DAO_HELPER_HRL).
-define(DAO_HELPER_HRL, 1).

-include_lib("veil_modules/dao/couch_db.hrl").

%% Those records represent view result Each #view_resault contains list of #view_row.
%% If the view has been queried with `include_docs` option, #view_row.doc will contain #veil_document, therefore
%% #view_row.id == #view_row.doc#veil_document.uuid. Unfortunately wrapping demanded record in #veil_document is
%% necessary because we may need revision info for that document.
-record(view_row, {id = "", key = "", value = 0, doc = none}).
-record(view_result, {total = 0, offset = 0, rows = []}).

-endif.