%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_errors.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_errors_tests).
-author("Rafal Slota").

-include("veil_modules/fslogic/fslogic.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

gen_error_code_test() ->
    ?assertMatch({?VEEXIST, _}, fslogic_errors:gen_error_code(?VEEXIST)),
    ?assertMatch({?VEREMOTEIO, _}, fslogic_errors:gen_error_code("invalid error code")),
    ?assertMatch({?VENOENT, some_details}, fslogic_errors:gen_error_code({?VENOENT, some_details})),

    ?assertMatch({?VENOENT, _}, fslogic_errors:gen_error_code(file_not_found)),
    ?assertMatch({?VEACCES, {permission_denied, some_details}}, fslogic_errors:gen_error_code({permission_denied, some_details})),
    ?assertMatch({?VEPERM, user_not_found}, fslogic_errors:gen_error_code(user_not_found)),
    ?assertMatch({?VEPERM, user_doc_not_found}, fslogic_errors:gen_error_code(user_doc_not_found)),
    ?assertMatch({?VEACCES, invalid_group_access}, fslogic_errors:gen_error_code(invalid_group_access)),
    ?assertMatch({?VEEXIST, _}, fslogic_errors:gen_error_code(file_exists)).

normalize_error_code_test() ->
    ?assertMatch("code", fslogic_errors:normalize_error_code(code)),
    ?assertMatch("code", fslogic_errors:normalize_error_code("code")).

-endif.
