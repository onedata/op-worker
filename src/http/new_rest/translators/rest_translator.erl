%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of request results to REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(rest_translator).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("http/rest/rest.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([response/2, error_response/1]).

%% Convenience functions for rest translators
-export([
    created_reply/2,
    ok_no_content_reply/0,
    ok_body_reply/1,
    updated_reply/0,
    deleted_reply/0
]).

%%%===================================================================
%%% API
%%%===================================================================


-spec response(_, term()) -> #rest_resp{}.
response(_, {error, _} = Err) ->
    error_response(Err);
response(#op_req{operation = create}, ok) ->
    % No need for translation, 'ok' means success with no response data
    rest_translator:ok_no_content_reply();
response(#op_req{operation = create} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}, auth_hint = AuthHint} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:create_response(GRI, AuthHint, DataFormat, Result);
response(#op_req{operation = get} = OpReq, {ok, Data}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:get_response(GRI, Data);
response(#op_req{operation = update}, ok) ->
    updated_reply();
response(#op_req{operation = delete}, ok) ->
    deleted_reply();
response(#op_req{operation = delete} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:delete_response(GRI, DataFormat, Result).


%%--------------------------------------------------------------------
%% @doc
%% Translates an entity logic error into REST response.
%% @end
%%--------------------------------------------------------------------
-spec error_response({error, term()}) -> #rest_resp{}.
error_response({error, Type}) ->
    case translate_error({error, Type}) of
        Code when is_integer(Code) ->
            #rest_resp{code = Code};
        {Code, {MessageFormat, FormatArgs}} ->
            MessageBinary = str_utils:format_bin(
                str_utils:to_list(MessageFormat), FormatArgs
            ),
            #rest_resp{code = Code, body = #{<<"error">> => MessageBinary}};
        {Code, MessageBinary} ->
            #rest_resp{code = Code, body = #{<<"error">> =>  MessageBinary}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% REST reply that should be used for successful REST operations that send
%% a body in response.
%% @end
%%--------------------------------------------------------------------
-spec ok_body_reply(json_utils:json_term()) -> #rest_resp{}.
ok_body_reply(Body) ->
    #rest_resp{code = ?HTTP_200_OK, body = Body}.


%%--------------------------------------------------------------------
%% @doc
%% REST reply that should be used for successful REST operations that do not
%% send any body in response.
%% @end
%%--------------------------------------------------------------------
-spec ok_no_content_reply() -> #rest_resp{}.
ok_no_content_reply() ->
    #rest_resp{code = ?HTTP_204_NO_CONTENT}.


%%--------------------------------------------------------------------
%% @doc
%% REST reply that should be used for successful create REST calls.
%% Returns 201 CREATED with proper location headers.
%% @end
%%--------------------------------------------------------------------
-spec created_reply(PathTokens :: [binary()], json_utils:json_term()) ->
    #rest_resp{}.
% Make sure there is no leading slash (so filename can be used for joining path)
created_reply([<<"/", Path/binary>> | Tail], Body) ->
    created_reply([Path | Tail], Body);
created_reply(PathTokens, Body) ->
    <<"/", Path/binary>> = filename:join([<<"/">> | PathTokens]),
    LocationHeader = #{
        <<"Location">> => list_to_binary(oneprovider:get_rest_endpoint(Path))
    },
    #rest_resp{code = ?HTTP_200_OK, headers = LocationHeader, body = Body}.


%%--------------------------------------------------------------------
%% @doc
%% REST reply that should be used for successful REST updates.
%% @end
%%--------------------------------------------------------------------
-spec updated_reply() -> #rest_resp{}.
updated_reply() ->
    #rest_resp{code = ?HTTP_204_NO_CONTENT}.


%%--------------------------------------------------------------------
%% @doc
%% REST reply that should be used for successful REST deletions.
%% @end
%%--------------------------------------------------------------------
-spec deleted_reply() -> #rest_resp{}.
deleted_reply() ->
    #rest_resp{code = ?HTTP_204_NO_CONTENT}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec entity_type_to_translator(atom()) -> module().
entity_type_to_translator(op_file) -> file_rest_translator;
entity_type_to_translator(op_metrics) -> metrics_rest_translator;
entity_type_to_translator(op_provider) -> provider_rest_translator;
entity_type_to_translator(op_replica) -> replica_rest_translator;
entity_type_to_translator(op_share) -> share_rest_translator;
entity_type_to_translator(op_space) -> space_rest_translator;
entity_type_to_translator(op_transfer) -> transfer_rest_translator.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates an entity logic error into HTTP code, headers and body.
%% @end
%%--------------------------------------------------------------------
-spec translate_error({error, term()}) ->
    Code |
    {Code, {MessageFormat :: binary(), FormatArgs :: [term()]}} |
    {Code, MessageBinary :: binary()} when Code :: integer().
% General errors
translate_error(?ERROR_INTERNAL_SERVER_ERROR) ->
    ?HTTP_500_INTERNAL_SERVER_ERROR;

translate_error(?ERROR_NOT_IMPLEMENTED) ->
    ?HTTP_501_NOT_IMPLEMENTED;

translate_error(?ERROR_NOT_SUPPORTED) ->
    {?HTTP_400_BAD_REQUEST,
        <<"This operation is not supported">>
    };

translate_error(?ERROR_ALREADY_EXISTS) ->
    {?HTTP_404_NOT_FOUND,
        <<"Given resource already exists">>
    };

translate_error(?ERROR_NOT_FOUND) ->
    ?HTTP_404_NOT_FOUND;

translate_error(?ERROR_UNAUTHORIZED) ->
    ?HTTP_401_UNAUTHORIZED;

translate_error(?ERROR_FORBIDDEN) ->
    ?HTTP_403_FORBIDDEN;

translate_error({error, ?EACCES}) ->
    ?HTTP_403_FORBIDDEN;

% Errors connected with macaroons
translate_error(?ERROR_BAD_MACAROON) ->
    {?HTTP_401_UNAUTHORIZED,
        <<"Provided authorization token could not be understood by the server">>
    };
translate_error(?ERROR_MACAROON_INVALID) ->
    {?HTTP_401_UNAUTHORIZED,
        <<"Provided authorization token is not valid">>
    };
translate_error(?ERROR_MACAROON_EXPIRED) ->
    {?HTTP_401_UNAUTHORIZED,
        <<"Provided authorization token has expired">>
    };
translate_error(?ERROR_MACAROON_TTL_TO_LONG(MaxTtl)) ->
    {?HTTP_401_UNAUTHORIZED,
        <<"Provided authorization token has too long (insecure) TTL (it must not exceed ~B seconds)">>,
        [MaxTtl]
    };

% Errors connected with bad data
translate_error(?ERROR_MALFORMED_DATA) ->
    {?HTTP_400_BAD_REQUEST,
        <<"Provided data could not be understood by the server">>
    };
translate_error(?ERROR_BAD_BASIC_CREDENTIALS) ->
    {?HTTP_401_UNAUTHORIZED,
        <<"Provided basic authorization credentials are not valid">>
    };
translate_error(?ERROR_MISSING_REQUIRED_VALUE(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Missing required value: ~s">>, [Key]}
    };
translate_error(?ERROR_MISSING_AT_LEAST_ONE_VALUE(Keys)) ->
    KeysList = str_utils:join_binary(Keys, <<", ">>),
    {?HTTP_400_BAD_REQUEST,
        {<<"Missing data, you must provide at least one of: ">>, [KeysList]}
    };
translate_error(?ERROR_BAD_DATA(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" could not be understood by the server">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_EMPTY(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must not be empty">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_ATOM(Key)) ->
    % Atoms are strings in json
    translate_error(?ERROR_BAD_VALUE_BINARY(Key));
translate_error(?ERROR_BAD_VALUE_BOOLEAN(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be a boolean">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_LIST_OF_ATOMS(Key)) ->
    % Atoms are strings in json
    translate_error(?ERROR_BAD_VALUE_LIST_OF_BINARIES(Key));
translate_error(?ERROR_BAD_VALUE_BINARY(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be a string">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_LIST_OF_BINARIES(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be a list of strings">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_INTEGER(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be an integer">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_FLOAT(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be a floating point number">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_JSON(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be a valid JSON">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_TOKEN(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" is not a valid token">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_LIST_OF_IPV4_ADDRESSES(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" is not a valid list of IPv4 addresses">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_DIRECTORY) ->
    {?HTTP_400_BAD_REQUEST,
        <<"Given path or id does not refer to a directory">>
    };
translate_error(?ERROR_BAD_VALUE_DOMAIN(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" is not a valid domain">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_SUBDOMAIN) ->
    {?HTTP_400_BAD_REQUEST,
        <<"Bad value: provided subdomain is not valid">>
    };
translate_error(?ERROR_BAD_VALUE_TOO_LOW(Key, Threshold)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be at least ~B">>, [Key, Threshold]}
    };
translate_error(?ERROR_BAD_VALUE_TOO_HIGH(Key, Threshold)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must not exceed ~B">>, [Key, Threshold]}
    };
translate_error(?ERROR_BAD_VALUE_NOT_IN_RANGE(Key, Low, High)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" must be between <~B, ~B>">>, [Key, Low, High]}
    };
translate_error(?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedValues)) ->
    % Convert binaries to strings so that
    % we do not include << >> signs in the response.
    AllowedValuesNotBin = lists:map(
        fun(Val) -> case Val of
            Bin when is_binary(Bin) -> binary_to_list(Bin);
            (Val) -> Val
        end end, AllowedValues),
    {?HTTP_400_BAD_REQUEST, {
        <<"Bad value: provided \"~s\" must be one of: ~p">>,
        [Key, AllowedValuesNotBin]
    }};
translate_error(?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(Key, AllowedValues)) ->
    % Convert binaries to strings so that we do not include << >> signs in the response.
    AllowedValuesNotBin = lists:map(
        fun(Val) when is_binary(Val) -> binary_to_list(Val);
            (Val) -> Val
        end, AllowedValues),
    {?HTTP_400_BAD_REQUEST, {
        <<"Bad value: provided \"~s\" must be a list containing zero or more following values: ~p">>,
        [Key, AllowedValuesNotBin]
    }};
translate_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided ID (\"~s\") does not exist">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_AMBIGUOUS_ID(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided (\"~s\") is ambiguous and does not explicitly indicate the given resource.">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_IDENTIFIER_OCCUPIED(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        % until VFS-3817 has been resolved do not change this description
        % as it is used to identify this specific error
        {<<"Bad value: provided identifier (\"~s\") is already occupied">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_BAD_TOKEN_TYPE(Key)) ->
    {?HTTP_400_BAD_REQUEST,
        {<<"Bad value: provided \"~s\" is of invalid type">>, [Key]}
    };
translate_error(?ERROR_BAD_VALUE_NAME) ->
    {?HTTP_400_BAD_REQUEST, <<
        "Bad value: Name must be 2-50 characters long and composed of UTF-8 letters, digits, brackets and underscores."
        "Dashes, spaces and dots are allowed (but not at the beginning or the end)."
    >>};
translate_error(?ERROR_BAD_VALUE_IDENTIFIER(Key)) ->
    {?HTTP_400_BAD_REQUEST, {
        <<"Bad value: provided \"~s\" is not a valid identifier.">>, [Key]
    }};

% Errors connected with relations between entities
translate_error(?ERROR_RELATION_DOES_NOT_EXIST(ChType, ChId, ParType, ParId)) ->
    RelationToString = case {ChType, ParType} of
        {od_space, od_provider} -> <<"is not supported by">>;
        {_, _} -> <<"is not a member of">>
    end,
    {?HTTP_400_BAD_REQUEST, {<<"Bad value: ~s ~s ~s">>, [
        ChType:to_string(ChId),
        RelationToString,
        ParType:to_string(ParId)
    ]}};
translate_error(?ERROR_RELATION_ALREADY_EXISTS(ChType, ChId, ParType, ParId)) ->
    RelationToString = case {ChType, ParType} of
        {od_space, od_provider} -> <<"is alraedy supported by">>;
        {_, _} -> <<"is already a member of">>
    end,
    {?HTTP_400_BAD_REQUEST, {<<"Bad value: ~s ~s ~s">>, [
        ChType:to_string(ChId),
        RelationToString,
        ParType:to_string(ParId)
    ]}};
translate_error(?ERROR_CANNOT_DELETE_ENTITY(EntityType, EntityId)) ->
    {?HTTP_500_INTERNAL_SERVER_ERROR, {
        <<"Cannot delete ~s, failed to delete some dependent relations">>,
        [EntityType:to_string(EntityId)]
    }};
translate_error(?ERROR_SUBDOMAIN_DELEGATION_DISABLED) ->
    {?HTTP_400_BAD_REQUEST, <<"Subdomain delegation is currently disabled.">>};
translate_error(?ERROR_BAD_VALUE_EMAIL) ->
    {?HTTP_400_BAD_REQUEST, <<"Bad value: provided e-mail is not a valid e-mail.">>};
translate_error(?ERROR_SPACE_NOT_SUPPORTED) ->
    {?HTTP_400_BAD_REQUEST, <<"The space of requested resource is not locally supported.">>};
translate_error(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId)) ->
    {?HTTP_400_BAD_REQUEST, {
        <<"The space of requested resource is not supported by ~s provider.">>, [ProviderId]
    }};
translate_error(?ERROR_INDEX_NOT_SUPPORTED_BY(ProviderId)) ->
    {?HTTP_400_BAD_REQUEST, {
        <<"The specified index is not supported by ~s provider.">>, [ProviderId]
    }};

% Errors associated with transfers
translate_error(?ERROR_TRANSFER_ALREADY_ENDED) ->
    {?HTTP_400_BAD_REQUEST, <<"Specified transfer has already ended.">>};
translate_error(?ERROR_TRANSFER_NOT_ENDED) ->
    {?HTTP_400_BAD_REQUEST, <<"Specified transfer has not ended yet.">>};

% Wildcard match
translate_error({error, Reason}) ->
    ?error("Unexpected error: {error, ~p} in rest error translator", [Reason]),
    translate_error(?ERROR_INTERNAL_SERVER_ERROR).
