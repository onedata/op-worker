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

-define(success_file_deleted, <<"FileDeleteSuccess">>).
-define(success_share_deleted, <<"ShareDeleteSuccess">>).
-define(success_file_uploaded, <<"UploadSuccess">>).

-define(error_user_unknown, <<"UserNonExistentInDB">>).
-define(error_path_unknown, <<"InvalidPath">>).
-define(error_version_unsupported, <<"APIVersionNotSupported">>).
-define(error_unknown, <<"UnknownError">>).
-define(error_no_id_in_uri, <<"MissingID">>).
-define(error_dir_cannot_delete, <<"DirDeletionForbidden">>).
-define(error_upload_unprocessable, <<"UnprocessableData">>).
-define(error_upload_cannot_create, <<"CannotCreateFile">>).
-define(error_share_cannot_create, <<"InvalidFilepath">>).

-define(code_to_message(ErrorCode),
    case ErrorCode of
        <<"FileDeleteSuccess">> -> <<"file deleted successfully">>;
        <<"ShareDeleteSuccess">> -> <<"share deleted successfully">>;
        <<"UploadSuccess">> -> <<"upload successful">>;
        <<"UserNonExistentInDB">> -> <<"the owner of supplied certificate doesn't exists in the database">>;
        <<"InvalidPath">> -> <<"requested path does not point to anything">>;
        <<"APIVersionNotSupported">> -> <<"requested API version is not supported">>;
        <<"UnknownError">> -> <<"unknown error">>;
        <<"MissingID">> -> <<"URI must specify a resource ID">>;
        <<"DirDeletionForbidden">> -> <<"cannot delete directories">>;
        <<"UnprocessableData">> -> <<"uprocessable multipart data">>;
        <<"CannotCreateFile">> -> <<"cannot create requested path (it is invalid or might already exist)">>;
        <<"InvalidFilepath">> -> <<"cannot create share (filepath is invalid)">>
    end).