%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module for accessing AmazonAWS IAM API
%%% @end
%%%-------------------------------------------------------------------
-module(amazonaws_iam).
-author("Michal Wrona").

-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([create_user/5, create_access_key/5, allow_access_to_bucket/6]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Creates new Amazon IAM user
%% @end
%%--------------------------------------------------------------------
-spec create_user(AdminAccessKey :: binary(), AdminSecretKey :: binary(),
    Host :: binary(), Region :: binary(), UserName :: binary()) ->
    ok | {error, Reason :: term()}.
create_user(AdminAccessKey, AdminSecretKey, Host, Region, UserName) ->
    case execute_iam_query(AdminAccessKey, AdminSecretKey, Host, Region,
        <<"CreateUser">>,
        [{<<"UserName">>, UserName}]) of
        {ok, 200, _, _} ->
            ok;
        {ok, 409, _, Body} ->
            case parse_error_response(Body) of
                {<<"EntityAlreadyExists">>, _} ->
                    ok;
                Error ->
                    {error, Error}
            end;
        {ok, _, _, Body} ->
            {error, parse_error_response(Body)}
    end.


%%--------------------------------------------------------------------
%% @doc Creates access key for given Amazon IAM user
%% @end
%%--------------------------------------------------------------------
-spec create_access_key(AdminAccessKey :: binary(), AdminSecretKey :: binary(),
    Host :: binary(), Region :: binary(), UserName :: binary()) ->
    {ok, {AccessKey :: binary(), SecretKey :: binary()}} | {error, Reason :: term()}.
create_access_key(AdminAccessKey, AdminSecretKey, Host, Region, UserName) ->
    case execute_iam_query(AdminAccessKey, AdminSecretKey, Host, Region,
        <<"CreateAccessKey">>,
        [{<<"UserName">>, UserName}]) of
        {ok, 200, _, Body} ->
            {XML, _} = xmerl_scan:string(binary_to_list(Body)),
            AccessKey = xml_val(
                xmerl_xpath:string(
                    "/CreateAccessKeyResponse/CreateAccessKeyResult/AccessKey/AccessKeyId", XML)),
            SecretKey = xml_val(
                xmerl_xpath:string(
                    "/CreateAccessKeyResponse/CreateAccessKeyResult/AccessKey/SecretAccessKey", XML)),
            {ok, {AccessKey, SecretKey}};
        {ok, _, _, Body} ->
            {error, parse_error_response(Body)}
    end.


%%--------------------------------------------------------------------
%% @doc Allows user access to bucket
%% @end
%%--------------------------------------------------------------------
-spec allow_access_to_bucket(AdminAccessKey :: binary(),
    AdminSecretKey :: binary(), Host :: binary(), Region :: binary(),
    UserName :: binary(), BucketName :: binary()) ->
    ok | {error, Reason :: term()}.
allow_access_to_bucket(AdminAccessKey, AdminSecretKey, Host, Region,
    UserName, BucketName) ->
    Policy = [{<<"Version">>, <<"2012-10-17">>},
        {<<"Statement">>, [
            [{<<"Effect">>, <<"Allow">>},
                {<<"Action">>, [<<"s3:ListBucket">>]},
                {<<"Resource">>, [<<"arn:aws:s3:::", BucketName/binary>>]}],
            [{<<"Effect">>, <<"Allow">>},
                {<<"Action">>,
                    [<<"s3:PutObject">>, <<"s3:GetObject">>, <<"s3:DeleteObject">>]},
                {<<"Resource">>, [<<"arn:aws:s3:::", BucketName/binary, "/*">>]}]]}],

    PolicyDocumentString = binary_to_list(json_utils:encode(Policy)),
    PolicyDocumentEscaped = list_to_binary(http_uri:encode(PolicyDocumentString)),
    %% Necessary because Amazon expects '*' encoded as '%2A'
    %% which is not done by http_uri:encode
    PolicyDocument = re:replace(PolicyDocumentEscaped, <<"[*]">>, <<"%2A">>,
        [{return, binary}, global]),

    case execute_iam_query(AdminAccessKey, AdminSecretKey, Host, Region,
        <<"PutUserPolicy">>, [{<<"UserName">>, UserName}, {<<"PolicyName">>,
            <<BucketName/binary, "-access">>},
            {<<"PolicyDocument">>, PolicyDocument}]) of
        {ok, 200, _, _} ->
            ok;
        {ok, _, _, Body} ->
            {error, parse_error_response(Body)}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Creates necessary authorization params and
%% executes query to Amazon IAM
%% More info about authentication process:
%% http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
%% @end
%%--------------------------------------------------------------------
-spec execute_iam_query(AccessKey :: binary(), SecretKey :: binary(),
    Host :: binary(), Region :: binary(), Action :: binary(),
    Params :: [{binary(), binary()}]) ->
    {ok, integer(), list(), binary()} |{error, Reason :: term()}.
execute_iam_query(AccessKey, SecretKey, Host, Region, Action, Params) ->
    {Date, Time} = erlang:universaltime(),
    DatestampString = lists:flatten(io_lib:format("~4.10.0b~2.10.0b~2.10.0b",
        tuple_to_list(Date))),
    Datestamp = list_to_binary(DatestampString),
    AmzDateString = lists:flatten(io_lib:format(
        "~4.10.0b~2.10.0b~2.10.0bT~2.10.0b~2.10.0b~2.10.0bZ",
        tuple_to_list(Date) ++ tuple_to_list(Time))),
    AmzDate = list_to_binary(AmzDateString),

    Method = <<"GET">>,
    Service = <<"iam">>,
    CanonicalURI = <<"/">>,
    CanonicalHeaders = <<"host:", Host/binary, "\n">>,
    SignedHeaders = <<"host">>,
    Algorithm = <<"AWS4-HMAC-SHA256">>,
    CredentialScope = <<Datestamp/binary, "/", Region/binary, "/",
        Service/binary, "/aws4_request">>,
    AmzCredentialsString = binary_to_list(<<AccessKey/binary, "/",
        CredentialScope/binary>>),
    AmzCredentials = list_to_binary(http_uri:encode(AmzCredentialsString)),

    SortedParams = lists:sort(Params ++ [
        {<<"Version">>, <<"2010-05-08">>},
        {<<"X-Amz-Algorithm">>, <<"AWS4-HMAC-SHA256">>},
        {<<"X-Amz-Credential">>, AmzCredentials},
        {<<"X-Amz-Date">>, AmzDate},
        {<<"X-Amz-Expires">>, <<"30">>},
        {<<"X-Amz-SignedHeaders">>, SignedHeaders}
    ]),

    CanonicalQuerystring = lists:foldl(fun({Key, Val}, Acc) ->
        <<Acc/binary, "&", Key/binary, "=", Val/binary>> end,
        <<"Action=", Action/binary>>, SortedParams),

    PayloadHash = list_to_binary(hex_utils:to_hex(crypto:hash(sha256, ""))),
    CanonicalRequest = <<Method/binary, "\n", CanonicalURI/binary,
        "\n", CanonicalQuerystring/binary, "\n",
        CanonicalHeaders/binary, "\n", SignedHeaders/binary, "\n",
        PayloadHash/binary>>,
    CanonicalRequestHash = list_to_binary(hex_utils:to_hex(crypto:hash(sha256,
        CanonicalRequest))),
    StringToSign = <<Algorithm/binary, "\n", AmzDate/binary, "\n",
        CredentialScope/binary, "\n",
        CanonicalRequestHash/binary>>,

    SigningKey = get_signature_key(SecretKey, Datestamp, Region, Service),
    Signature = list_to_binary(hex_utils:to_hex(crypto:hmac(sha256, SigningKey,
        StringToSign))),

    FinalQuerystring = <<CanonicalQuerystring/binary, "&X-Amz-Signature=",
        Signature/binary>>,

    RequestURL = <<"https://", Host/binary, CanonicalURI/binary, "?",
        FinalQuerystring/binary>>,
    %% TODO VFS-1674 Change hackney use for Amazon IAM to http_client
    {ok, Status, Headers, Ref} = hackney:request(get, RequestURL, [],
        <<"">>, [insecure]),
    {ok, Body} = hackney:body(Ref),
    {ok, Status, Headers, Body}.


%%--------------------------------------------------------------------
%% @doc Generates key to sign IAM Amazon request
%% @end
%%--------------------------------------------------------------------
-spec get_signature_key(SecretKey :: binary(), Datestamp :: binary(),
    Region :: binary(), Service :: binary()) -> binary().
get_signature_key(SecretKey, Datestamp, Region, Service) ->
    KDate = crypto:hmac(sha256, <<"AWS4", SecretKey/binary>>, Datestamp),
    KRegion = crypto:hmac(sha256, KDate, Region),
    KService = crypto:hmac(sha256, KRegion, Service),
    crypto:hmac(sha256, KService, <<"aws4_request">>).


%%--------------------------------------------------------------------
%% @doc Parses error response from Amazon IAM
%% @end
%%--------------------------------------------------------------------
-spec parse_error_response(Body :: binary()) ->
    {Code :: binary(), Message :: binary()}.
parse_error_response(Body) ->
    {XML, _} = xmerl_scan:string(binary_to_list(Body)),
    Code = xml_val(xmerl_xpath:string("/ErrorResponse/Error/Code", XML)),
    Message = xml_val(xmerl_xpath:string("/ErrorResponse/Error/Message", XML)),
    {Code, Message}.


%%--------------------------------------------------------------------
%% @doc Extracts value from xml
%% @end
%%--------------------------------------------------------------------
-spec xml_val(Doc :: [#xmlElement{}]) -> binary().
xml_val(Doc) ->
    [#xmlElement{content = [#xmlText{value = V} | _]}] = Doc,
    list_to_binary(V).
