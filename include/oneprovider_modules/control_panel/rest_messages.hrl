%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This hrl contains non-error messages returned as replies to REST requests.
%% These macros can be used directly in rest_utils:success_reply/1.
%% They must evaluate to {binary(), binary()} tuples.
%% @end
%% ===================================================================

-define(success_file_deleted, {<<"FileDeleteSuccess">>, <<"file deleted successfully">>}).
-define(success_file_uploaded, {<<"UploadSuccess">>, <<"upload successful">>}).
-define(success_share_deleted, {<<"ShareDeleteSuccess">>, <<"share deleted successfully">>}).