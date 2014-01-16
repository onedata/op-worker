%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This hrl aggregates all messages that can be returned as replies to
%% REST requests. No messages should be hardcoded in rest modules.
%% @end
%% ===================================================================

-define(success_file_deleted, <<"file deleted successfully">>).
-define(success_share_deleted, <<"share deleted successfully">>).
-define(success_file_uploaded, <<"upload successful">>).


-define(error_unknown, <<"unknown error">>).
-define(error_no_id_in_uri, <<"URI must specify a resource ID">>).
-define(error_cannot_delete_dir, <<"cannot delete directories">>).
-define(error_upload_unprocessable, <<"uprocessable multipart data">>).
-define(error_upload_cannot_create, <<"cannot create requested path (it is invalid or might already exist)">>).
-define(error_share_cannot_create, <<"cannot create share (filepath is invalid)">>).