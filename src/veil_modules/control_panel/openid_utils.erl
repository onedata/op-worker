%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a simple library used to establish an OpenID authentication.
%% It needs nitrogen and ibrowse to run
%% @end
%% ===================================================================

-module(openid_utils).

-include_lib("xmerl/include/xmerl.hrl"). 
-include_lib("ibrowse/include/ibrowse.hrl").
-include("veil_modules/control_panel/openid.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([get_login_url/0, validate_openid_login/0]).


%% get_login_url/0
%% ====================================================================
%% @doc
%% Produces an URL with proper GET parameters,
%% used to redirect the user to OpenID Provider login page.
%% @end
-spec get_login_url() -> Result when
  Result :: ok | {error, endpoint_unavailable}.
%% ====================================================================
get_login_url() ->
	try 
		discover_op_endpoint(?xrds_url) ++ 
		"?openid.mode=checkid_setup" ++ 
		"&" ++ ?openid_ns ++
		"&" ++ ?openid_return_to ++
		"&" ++ ?openid_claimed_id ++
		"&" ++ ?openid_identity ++
		"&" ++ ?openid_realm ++
		"&" ++ ?openid_sreg_required
	catch _:Message ->
		lager:error("Unable to resolve OpenID Provider endpoint.~n~p~n~p", [Message, erlang:get_stacktrace()]),
		{error, endpoint_unavailable}
	end.



%% validate_openid_login/0
%% ====================================================================
%% @doc
%% Checks if parameters returned from OP were really generated by them.
%% Upon success, returns a proplist with information about the user.
%% @end
-spec validate_openid_login() -> Result when
  Result :: {ok, list()} | {error, Error},
  Error :: missing_credentials | auth_invalid | auth_failed.
%% ====================================================================
validate_openid_login() ->	
	% Extract GET parameters
	Parameters = wf:mqs(?openid_login_response_key_list),
	% And make a proplist
	Proplist = 	try lists:zipwith(
					fun (Key, [Value]) -> {Key, Value}; 
						(Key, [Value|_WfMqsHasABug]) -> {Key, Value} 
					end, ?openid_login_response_key_list, Parameters)
				catch _:_ ->
					invalid
				end,

	try 
		case Proplist of 
			% There were no credentials in GET params
			invalid ->
				{error, missing_credentials};
			_ ->
				RequestParameters = lists:foldl(
					fun({Key, Value}, Acc) ->
						case Key of
							% Skip keys that are not needed in check_authentication
							'openid.ns' -> Acc;
							'openid.mode' -> Acc;
							% URL-encode problematic characters
							'openid.sreg.fullname' -> Acc ++ "&" ++ "openid.sreg.fullname" ++ "=" ++ wf:url_encode(Value);
							'openid.sig' -> Acc ++ "&" ++ "openid.sig" ++ "=" ++ wf:url_encode(Value);
							OtherKey -> Acc ++ "&" ++ atom_to_list(OtherKey) ++ "=" ++ Value
						end
					end, [], Proplist),

				EndpointURL = proplists:get_value('openid.op_endpoint', Proplist),
				ValidationRequestBody =	"openid.mode=check_authentication" ++ RequestParameters,
				case check_authentication(EndpointURL, ValidationRequestBody) of
					?valid_auth_info ->
						Nickname = proplists:get_value('openid.sreg.nickname', Proplist),
						Email = proplists:get_value('openid.sreg.email', Proplist),
						FullName = proplists:get_value('openid.sreg.fullname', Proplist),
						{ok, [{nickname, Nickname}, {email, Email}, {fullname, FullName}]};
					?invalid_auth_info ->
						lager:alert("Security breach attempt spotted. Invalid redirect URL contained: ~p", [Proplist]),
						{error, auth_invalid}
				end
		end
	catch _:Message ->
		lager:error("Unable to validate credentials served by OpenID Provider.~n~p~n~p", [Message, erlang:get_stacktrace()]),
		{error, auth_failed}
	end.


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

% Retrieve and parse an XRDS document 
discover_op_endpoint(EndpointURL) ->
	XRDS = get_xrds(EndpointURL),
 	{Xml, _} = xmerl_scan:string(XRDS),
	xml_extract_value("URI", Xml).


% Extract value having a certain key
xml_extract_value(KeyName, Xml) ->	
	[#xmlElement{content = [#xmlText{value = Value}|_]}] = xmerl_xpath:string("//" ++ KeyName, Xml),
	Value.	


% Send a POST check-authentication request to check if it's valid
check_authentication(EndpointURL, ValidationRequestBody) ->
    Raw = ibrowse:send_req(EndpointURL, [{content_type, "application/x-www-form-urlencoded"}], post, ValidationRequestBody),
    {ok, _, _, Response} = normalise_response(Raw),   
	Response.


% Get xrds file
get_xrds(URL) ->
	% Maximum redirection count = 5
    {ok, _, _, Body} = get_xrds(URL, 5),
    Body.

% Get xrds file performing GET on provided URL
get_xrds(URL, Redirects) ->
    ReqHeaders = 
    [
    	{"Accept", "application/xrds+xml;level=1, */*"},
		{"Connection", "close"},
		{"User-Agent", "Erlang/erl_openid"}
	],
    ResponseRaw = ibrowse:send_req(URL, ReqHeaders, get),
    Response = normalise_response(ResponseRaw),
    case Response of
		{ok, Rcode, RespHeaders, _Body}	when Rcode > 300 andalso Rcode < 304 andalso Redirects > 0 ->
			case get_redirect_url(URL, RespHeaders) of
				undefined -> Response;
				URL -> Response;
				NewURL -> get_xrds(NewURL, Redirects - 1)
			end;
		Response -> Response
    end.

% Parse out redirect URL from response
get_redirect_url(OldURL, Headers) ->
	Location = proplists:get_value("location", Headers),
	case Location of
		"http://" ++ _ -> Location;
		"https://" ++ _ -> Location;
		[$/|_] = Location ->
			#url{protocol=Protocol, host=Host, port=Port} = ibrowse_lib:parse_url(OldURL),
			PortFrag = 	case {Protocol, Port} of
							{http, 80} -> "";
							{https, 443} -> "";
							_ -> ":" ++ integer_to_list(Port)
						end,
		atom_to_list(Protocol) ++ "://" ++ Host ++ PortFrag ++ Location;
		_ -> undefined
	end.

% Make the response more readable
normalise_response({ok, RcodeList, Headers, Body}) ->
	RcodeInt = list_to_integer(RcodeList),
	LowHeaders = [ {string:to_lower(K), V} || {K, V} <- Headers ],
	{ok, RcodeInt, LowHeaders, Body};

normalise_response(X) -> X.