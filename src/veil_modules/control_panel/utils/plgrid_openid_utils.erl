%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a simple library used to establish an OpenID authentication.
%% It needs n2o context to run.
%% @end
%% ===================================================================
-module(plgrid_openid_utils).

-include_lib("xmerl/include/xmerl.hrl").
-include_lib("n2o/include/wf.hrl").
-include("veil_modules/control_panel/openid_utils.hrl").
-include_lib("ctool/include/logging.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([get_login_url/1, prepare_validation_parameters/0, validate_openid_login/1, retrieve_user_info/0]).


%% get_login_url/1
%% ====================================================================
%% @doc
%% Produces an URL with proper GET parameters,
%% used to redirect the user to OpenID Provider login page.
%% RedirectParams are parameters concatenated to return_to field.
%% @end
-spec get_login_url(HostName :: binary()) -> binary() | {error, endpoint_unavailable}.
%% ====================================================================
get_login_url(HostName) ->
    try
        Endpoint = discover_op_endpoint(?xrds_url),
        <<Endpoint/binary,
        "?", ?openid_checkid_mode,
        "&", ?openid_ns,
        "&", ?openid_return_to_prefix, HostName/binary, ?openid_return_to_suffix,
        "&", ?openid_claimed_id,
        "&", ?openid_identity,
        "&", ?openid_realm_prefix, HostName/binary,
        "&", ?openid_sreg_required,
        "&", ?openid_ns_ext1,
        "&", ?openid_ext1_mode,
        "&", ?openid_ext1_type_dn1,
        "&", ?openid_ext1_type_dn2,
        "&", ?openid_ext1_type_dn3,
        "&", ?openid_ext1_type_teams,
        "&", ?openid_ext1_if_available>>
    catch Type:Message ->
        ?error_stacktrace("Unable to resolve OpenID Provider endpoint - ~p: ~p", [Type, Message]),
        {error, endpoint_unavailable}
    end.


%% prepare_validation_parameters/0
%% ====================================================================
%% @doc
%% This function retrieves endpoint URL and parameters from redirection URL created by OpenID provider.
%% They are later used as arguments to validate_openid_login() function.
%% Must be called from within n2o page context to work, precisely
%% from openid redirection page.
%% @end
-spec prepare_validation_parameters() -> {string(), string()} | {error, invalid_request}.
%% ====================================================================
prepare_validation_parameters() ->
    try
        Params = gui_ctx:get_request_params(),
        % Make sure received endpoint is really the PLGrid endpoint
        EndpointURL = proplists:get_value(<<?openid_op_endpoint_key>>, Params),
        true = (discover_op_endpoint(?xrds_url) =:= EndpointURL),

        % 'openid.signed' contains parameters that must be contained in validation request
        SignedArgsNoPrefix = binary:split(proplists:get_value(<<?openid_signed_key>>, Params), <<",">>, [global]),
        % Add 'openid.' prefix to all parameters
        % And add 'openid.sig' and 'openid.signed' params which are required for validation
        SignedArgs = lists:map(
            fun(X) ->
                <<"openid.", X/binary>>
            end, SignedArgsNoPrefix) ++ [<<?openid_sig_key>>, <<?openid_signed_key>>],

        % Create a POST request body
        RequestParameters = lists:foldl(
            fun(Key, Acc) ->
                Value = case proplists:get_value(Key, Params) of
                            undefined -> throw("Value for " ++ gui_str:to_list(Key) ++ " not found");
                            Val -> Val
                        end,
                % Safely URL-decode params
                Param = gui_str:url_encode(Value),
                <<Acc/binary, "&", Key/binary, "=", Param/binary>>
            end, <<"">>, SignedArgs),
        ValidationRequestBody = <<?openid_check_authentication_mode, RequestParameters/binary>>,
        {gui_str:to_list(EndpointURL), gui_str:to_list(ValidationRequestBody)}

    catch Type:Message ->
        ?error_stacktrace("Failed to process login validation request - ~p: ~p", [Type, Message]),
        {error, invalid_request}
    end.


%% validate_openid_login/2
%% ====================================================================
%% @doc
%% Checks if parameters returned from OP were really generated by them.
%% Upon success, returns a proplist with information about the user.
%% Args must be properly prepared, eg. as in prepare_validation_parameters() function.
%% @end
-spec validate_openid_login({EndpointURL, ValidationRequestBody}) -> Result when
    EndpointURL :: string(),
    ValidationRequestBody :: string(),
    Result :: ok | {error, Error},
    Error :: auth_invalid | no_connection.
%% ====================================================================
validate_openid_login({EndpointURL, ValidationRequestBody}) ->
    try
        {ok, Response} = gui_utils:https_post(EndpointURL, [{"Content-Type", "application/x-www-form-urlencoded"}], ValidationRequestBody),
        case Response of
            <<?valid_auth_info>> -> ok;
            _ ->
                ?alert("Security breach attempt spotted. Invalid redirect URL contained:~n~p", [gui_ctx:get_request_params()]),
                {error, auth_invalid}
        end

    catch Type:Message ->
        ?error_stacktrace("Failed to connect to OpenID provider - ~p: ~p", [Type, Message]),
        {error, no_connection}
    end.


%% retrieve_user_info/0
%% ====================================================================
%% @doc
%% This function retrieves user info from parameters of redirection URL created by OpenID provider.
%% They are returned as a proplist and later used to authenticate a user in the system.
%% Must be called from within n2o page context to work, precisely
%% from openid redirection page.
%% @end
-spec retrieve_user_info() -> Result when
    Result :: {ok, list()} | {error, Error},
    Error :: invalid_request.
%% ====================================================================
retrieve_user_info() ->
    try
        Params = gui_ctx:get_request_params(),
        % Check which params were signed by PLGrid
        SignedParamsNoPrefix = binary:split(proplists:get_value(<<?openid_signed_key>>, Params), <<",">>, [global]),
        % Add 'openid.' prefix to all parameters
        % And add 'openid.sig' and 'openid.signed' params which are required for validation
        SignedParams = lists:map(
            fun(X) ->
                <<"openid.", X/binary>>
            end, SignedParamsNoPrefix),

        Login = get_signed_param(<<?openid_login_key>>, Params, SignedParams, unicode),
        % Login must be retrieved from OpenID, other info is not mandatory.
        case Login of
            [] -> throw(login_undefined);
            _ -> ok
        end,
        Name = get_signed_param(<<?openid_name_key>>, Params, SignedParams, unicode),
        Teams = parse_teams(get_signed_param(<<?openid_teams_key>>, Params, SignedParams, utf8)),
        Email = get_signed_param(<<?openid_email_key>>, Params, SignedParams, unicode),
        DN1 = get_signed_param(<<?openid_dn1_key>>, Params, SignedParams, unicode),
        DN2 = get_signed_param(<<?openid_dn2_key>>, Params, SignedParams, unicode),
        DN3 = get_signed_param(<<?openid_dn3_key>>, Params, SignedParams, unicode),
        DnList = lists:filter(
            fun(X) ->
                (X /= [])
            end, [DN1, DN2, DN3]),
        {ok, [
            {global_id, "plgrid__" ++ Login},
            {login, Login},
            {name, Name},
            {teams, Teams},
            {emails, [Email]},
            {dn_list, lists:usort(DnList)}
        ]}
    catch Type:Message ->
        ?error_stacktrace("Failed to retrieve user info - ~p: ~p~nOpenID redirect args were:~n~p",
            [Type, Message, gui_ctx:get_request_params()]),
        {error, invalid_request}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% get_signed_param/2
%% ====================================================================
%% @doc
%% Retrieves given request parameter, but only if it was signed by the provider.
%% Returns the param in desired encoding (unicode or utf8).
%% @end
-spec get_signed_param(ParamName :: binary(), POSTParams :: [{Key :: binary(), Value :: binary()}],
    SignedParams :: [binary()], Encoding :: unicode | utf8) -> string().
%% ====================================================================
get_signed_param(ParamName, POSTParams, SignedParams, Encoding) ->
    CoversionFun = case Encoding of
                       unicode -> fun(X) -> gui_str:binary_to_unicode_list(X) end;
                       utf8 -> fun(X) -> gui_str:to_list(X) end
                   end,
    case lists:member(ParamName, SignedParams) of
        true -> CoversionFun(proplists:get_value(ParamName, POSTParams, <<"">>));
        false -> []
    end.


%% discover_op_endpoint/1
%% ====================================================================
%% @doc
%% Retrieves an XRDS document from given endpoint URL and parses out the URI which will
%% be used for OpenID login redirect.
%% @end
-spec discover_op_endpoint(string()) -> binary().
%% ====================================================================
discover_op_endpoint(EndpointURL) ->
    XRDS = get_xrds(EndpointURL),
    {Xml, _} = xmerl_scan:string(XRDS),
    list_to_binary(xml_extract_value("URI", Xml)).


%% xml_extract_value/2
%% ====================================================================
%% @doc
%% Extracts value from under a certain key
%% @end
-spec xml_extract_value(string(), #xmlElement{}) -> string().
%% ====================================================================
xml_extract_value(KeyName, Xml) ->
    [#xmlElement{content = [#xmlText{value = Value} | _]}] = xmerl_xpath:string("//" ++ KeyName, Xml),
    Value.


%% get_xrds/1
%% ====================================================================
%% @doc
%% Downloads an XRDS document from given URL.
%% @end
-spec get_xrds(string()) -> string().
%% ====================================================================
get_xrds(URL) ->
    ReqHeaders =
        [
            {"Accept", "application/xrds+xml;level=1, */*"},
            {"Connection", "close"}
        ],
    {ok, XRDS} = gui_utils:https_get(URL, ReqHeaders),
    binary_to_list(XRDS).


%% parse_teams/1
%% ====================================================================
%% @doc
%% Parses user's teams from XML to a list of strings. Returns an empty list
%% for empty XML. NOTE! Returns a list of unicode strings.
%% @end
-spec parse_teams(string()) -> [string()].
%% ====================================================================
parse_teams([]) ->
    [];

parse_teams(XMLContent) ->
    {XML, _} = xmerl_scan:string(XMLContent),
    #xmlElement{content = TeamList} = find_XML_node(teams, XML),
    lists:map(
        fun(#xmlElement{content = [#xmlText{value = Value}]}) ->
            Value
        end, TeamList).


%% find_XML_node/2
%% ====================================================================
%% @doc
%% Finds certain XML node. Assumes that node exists, and checks only
%% the first child of every node going deeper and deeper.
%% @end
-spec find_XML_node(atom(), #xmlElement{}) -> [string()].
%% ====================================================================
find_XML_node(NodeName, #xmlElement{name = NodeName} = XMLElement) ->
    XMLElement;

find_XML_node(NodeName, #xmlElement{} = XMLElement) ->
    [SubNode] = XMLElement#xmlElement.content,
    find_XML_node(NodeName, SubNode);

find_XML_node(_NodeName, _) ->
    undefined.


