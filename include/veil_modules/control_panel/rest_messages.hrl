%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This hrl aggregates all return codes and messages that can be
%% returned as replies to REST requests. All macros should be evaluated
%% to a tuple of form {Code, Message}, where both code and message are binaries.
%% @end
%% ===================================================================

-define(success_file_deleted, {<<"FileDeleteSuccess">>, <<"file deleted successfully">>}).
-define(success_share_deleted, {<<"ShareDeleteSuccess">>, <<"share deleted successfully">>}).
-define(success_file_uploaded, {<<"UploadSuccess">>, <<"upload successful">>}).


-define(error_user_unknown, {<<"UserNonExistentInDB">>, <<"the owner of supplied certificate doesn't exists in the database">>}).
-define(error_path_unknown, {<<"InvalidPath">>, <<"requested path does not point to anything">>}).
-define(error_version_unsupported, {<<"APIVersionNotSupported">>, <<"requested API version is not supported">>}).
-define(error_unknown, {<<"UnknownError">>, <<"unknown error">>}).
-define(error_no_id_in_uri, {<<"MissingID">>, <<"URI must specify a resource ID">>}).
-define(error_dir_cannot_delete, {<<"DirDeletionForbidden">>, <<"cannot delete directories">>}).
-define(error_upload_unprocessable, {<<"UnprocessableData">>, <<"uprocessable multipart data">>}).
-define(error_upload_cannot_create, {<<"CannotCreateFile">>, <<"cannot create requested path (it is invalid or might already exist)">>}).
-define(error_share_cannot_create, {<<"InvalidFilepath">>, <<"cannot create share (filepath is invalid)">>}).