%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This hrl aggregates all error codes that can occur in the system.
%% All macros should be evaluated to a tuple of form {Code, Message},
%% where both code and message are strings. Messages can contain formatting syntax
%% like in io_lib:format (e.g. "file ~p deleted"). If they do, they must be used
%% with suitable list of parameters or an exception will be raised otherwise.
%% @end
%% ===================================================================

% Note, logging.hrl is automaticaly include wherever err.hrl is.
-include("logging.hrl").

-type status() :: warning | error | alert.


% Record representing an error
-record(error_rec, {
    status = error :: status(),
    code = "" :: string(),
    description = "" :: string()
}).


% These macros are {Code, Message} tuples. Messages can contain formatting chars.
-define(error_user_unknown, {"UserNonExistentInDB", "the owner of supplied certificate doesn't exists in the database: \"~s\""}).
-define(error_path_invalid, {"InvalidPath", "requested path does not point to anything"}).
-define(error_version_unsupported, {"APIVersionNotSupported", "API version '~s' is not supported"}).
-define(error_method_unsupported, {"MethodNotSupported", "Method '~s' is not supported"}).
-define(error_media_type_unsupported, {"MediaTypeNotSupported", "Accepting only: application/json, application/x-www-form-urlencoded, multipart/form-data"}).
-define(error_no_id_in_uri, {"MissingID", "URI must specify a resource ID"}).
-define(error_not_found, {"NotFound", "Resource of requested ID not found: ~s"}).
-define(error_dir_cannot_delete, {"DirDeletionForbidden", "cannot delete directories"}).
-define(error_reg_file_cannot_delete, {"FileDeletionError", "unable to delete regular file ~s"}).
-define(error_upload_unprocessable, {"UnprocessableData", "uprocessable multipart data"}).
-define(error_upload_cannot_create, {"CannotCreateFile", "cannot create requested path (it is invalid or might already exist)"}).
-define(error_share_cannot_create, {"ShareCannotCreate", "cannot create share - filepath is invalid: ~s"}).
-define(error_share_cannot_retrieve, {"ShareCannotRetrieve", "cannot retrieve share info based on uuid: ~s"}).
-define(error_share_cannot_delete, {"ShareCannotDelete", "cannot delete share by uuid: ~s"}).

% ?report_xxx macros should be used with one of above as second argument.
% ?report_xxx/3 is used when no formatting args are needed, ?report_xxx/4 otherwise.
% It returns an error_rec, representing the error.
-define(report_warning(_ErrDesc),
    ?report_warning(_ErrDesc, [])
).

-define(report_warning(_ErrDesc, _Args),
    begin
        ?warning(element(2, _ErrDesc), _Args),
        #error_rec{status = warning, code = element(1, _ErrDesc), description = lists:flatten(io_lib:format(element(2, _ErrDesc), _Args))}
    end
).


-define(report_error(_ErrDesc),
    ?report_error(_ErrDesc, [])
).

-define(report_error(_ErrDesc, _Args),
    begin
        ?error(element(2, _ErrDesc), _Args),
        #error_rec{status = error, code = element(1, _ErrDesc), description = lists:flatten(io_lib:format(element(2, _ErrDesc), _Args))}
    end
).


-define(report_alert(_ErrDesc),
    ?report_alert(_ErrDesc, [])
).

-define(report_alert(_ErrDesc, _Args),
    begin
        ?alert(element(2, _ErrDesc), _Args),
        #error_rec{status = alert, code = element(1, _ErrDesc), description = lists:flatten(io_lib:format(element(2, _ErrDesc), _Args))}
    end
).