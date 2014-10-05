%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides error handling functions for cdmi modules
%% @end
%% ===================================================================
-module(cdmi_error).

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_error.hrl").

%% API
-export([error_reply/3, error_reply/7]).

%% error_reply/3
%% ====================================================================
%% @doc  Prints error description log and sets cowboy reply according to ErrorName
%% @end
-spec error_reply(req(), #state{}, term()) -> {halt, req(), #state{}}.
%% ====================================================================
error_reply(Req, State, ErrorName) ->
    apply(?MODULE, error_reply, [Req, State] ++ error_value(ErrorName)).

%% error_reply/7
%% ====================================================================
%% @doc Prints error description log and sets cowboy reply to given body and error code
%% @end
-spec error_reply(req(), #state{}, integer(), list(), string(), list(), debug | info | warning | error) -> {halt, req(), #state{}}.
%% ====================================================================
error_reply(Req, State, ErrorCode, ReturnBody, ErrorDescription, DescriptionArgs, LogLevel) ->
    ?do_log(logger:loglevel_atom_to_int(LogLevel), "Handling request:~n~p~nwith state:~n~p~nend up with error:~n" ++ ErrorDescription, [Req, State] ++ DescriptionArgs, true),

    Body = case ReturnBody =:= [] of true -> []; _ -> rest_utils:encode_to_json(ReturnBody) end,
    {ok, Req2} = veil_cowboy_bridge:apply(cowboy_req, reply, [ErrorCode, [], Body, Req]),
    {halt, Req2, State}.

%% error_value/1
%% ====================================================================
%% @doc Returns Error code, body, log and log arguments depending on given error name
%% @end
-spec error_value(term()) -> {LogLevel :: debug | info | warning | error, {halt, req(), #state{}}}.
%% ====================================================================
error_value({?moved_permanently, Filepath}) ->
    [?moved_pemanently_code, [], "Wrong slash at the end of path: ~p", [Filepath], debug];

error_value(?duplicated_body_fields) ->
    [?error_bad_request_code, [{<<"BodyFieldsDuplicationError">>, <<"Body contains duplicated entries">>}], "Body contains duplicated entries", [], debug];
error_value(?conflicting_body_fields) ->
    [?error_bad_request_code, [{<<"BodyFieldsInConflictError">>, <<"Body contains a pair of conflicting entries">>}], "Body contains a pair of conflicting entries", [], debug];
error_value({?invalid_objectid, Id}) ->
    [?error_bad_request_code, [{<<"InvalidObjectIDError">>, <<"Could not decode objectid">>}], "Could not decode objectid: ~p", [Id], debug];
error_value(?invalid_content_type) ->
    [?error_bad_request_code, [{<<"InvalidContentTypeError">>, <<"Content type is invalid">>}], "Invalid content type", [], debug];
error_value({?invalid_range, RawRange}) ->
    [?error_bad_request_code, [{<<"InvalidRangeError">>, <<"Requested range is invalid">>}], "Invalid range: ~p", [RawRange], debug];
error_value(?invalid_childrenrange) ->
    [?error_bad_request_code, [{<<"InvalidChildrenrangeError">>, <<"Requested childrenrange is invalid">>}], "Invalid childrenrange", [], debug];
error_value(?no_version_given) ->
    [?error_bad_request_code, [{<<"NoVersionError">>, <<"The request does not have required X-Cdmi-Version header">>}], "No version specified", [], debug];
error_value(?invalid_base64) ->
    [?error_bad_request_code, [{<<"InvalidBase64Error">>, <<"Cannot convert base64 string">>}], "Invalid base64 string", [], debug];
error_value(?unsupported_version) ->
    [?error_bad_request_code, [{<<"UnsupportedVersionError">>, <<"Version unsupported">>}], "Invalid version error.", [], debug];
error_value({?malformed_request, Error}) ->
    [?error_bad_request_code, [{<<"MalformedRequestError">>, <<"The request is malformed">>}], "Malformed request error: ~p", [Error], debug];
error_value({?invalid_json, Error}) ->
    [?error_bad_request_code, [{<<"InvalidJsonError">>, <<"The json body could not be parsed">>}], "Malformed request error: ~p", [Error], debug];

error_value(?invalid_token) ->
    [?error_unauthorized_code, [{<<"InvalidTokenError">>, <<"The token is invalid or expired">>}], "Invalid token error", [], debug];
error_value(?invalid_cert) ->
    [?error_unauthorized_code, [{<<"CertificateError">>, <<"Cannot parse certificate">>}], "Invalid peer certificate error", [], debug];
error_value(?no_certificate_chain_found) ->
    [?error_unauthorized_code, [{<<"CertificateError">>, <<"Cannot find certificate chain">>}], "No certificate chain found error", [], debug];
error_value({?user_unknown, DnString}) ->
    [?error_unauthorized_code, [{<<"NoUserFoundError">>, <<"Cannot find user">>}], "No user found with given DN: ~p", [DnString], debug];

error_value(?space_dir_delete) ->
    [?error_forbidden_code, [{<<"SpaceDeleteError">>, <<"Deleting space directory, which is forbidden.">>}], "Deleting space dir", [], debug];
error_value(?forbidden) ->
    [?error_forbidden_code, [{<<"Forbidden">>, <<"Requested operation is forbidden.">>}], "Deleting space dir", [], debug];

error_value(?not_found) ->
    [?error_not_found_code, [], "Object not found", []];
error_value(?parent_not_found) ->
    [?error_not_found_code, [{<<"ParentNotFoundError">>, <<"Parent container could not be found">>}], "Parent container not found", [], debug];

error_value(?put_container_conflict) ->
    [?error_conflict_code, [{<<"PutContainerError">>, <<"Container already exists">>}], "Dir already exists", [], debug];

error_value({?get_attr_unknown_error, Error}) ->
    [?error_internal_code, [{<<"GetAttributesError">>, <<"Get attributes unknown error">>}], "Getting attributes end up with error: ~p", [Error], error];
error_value({?file_delete_unknown_error, Error}) ->
    [?error_internal_code, [{<<"DeleteError">>, <<"Object delete unknown error">>}], "Deleting file end up with error: ~p", [Error], error];
error_value({?put_container_unknown_error, Error}) ->
    [?error_internal_code, [{<<"PutContainerError">>, <<"Put container unknown error">>}], "Creating/updating container end up with error: ~p", [Error], error];
error_value({?dir_delete_unknown_error, Error}) ->
    [?error_internal_code, [{<<"DeleteError">>, <<"Container delete unknown error">>}], "Deleting dir end up with error: ~p", [Error], error];
error_value({?state_init_error, Error}) ->
    [?error_internal_code, [{<<"StateInitError">>, <<"Cannot initialize request state">>}], "State init error: ~p", [Error], error];
error_value({?put_object_unknown_error, Error}) ->
    [?error_internal_code, [{<<"PutObjectError">>, <<"Put object unknown error">>}], "Creating/updating cdmi object end up with error: ~p", [Error], error];
error_value({?write_object_unknown_error, Error}) ->
    [?error_internal_code, [{<<"WriteObjectError">>, <<"Write object unknown error">>}], "Writing to cdmi object end up with error: ~p", [Error], error];
error_value(ErrorName) ->
    [?error_internal_code, [{<<"InternalServerError">>, <<"Unknown internal server error">>}], "Unknown error name: ~p", [ErrorName], error].