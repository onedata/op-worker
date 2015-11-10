%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements some functions for parsing
%%% and processing parameters of request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_arg_parser).
-author("Piotr Ociepka").

-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").

-export([get_supported_version/1, parse_opts/1, malformed_request/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version and options and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {true|false, req(), #{}}.
malformed_request(Req, State) ->
  {RawVersion, Req2} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
  Version = get_supported_version(RawVersion),
  {Qs, Req3} = cowboy_req:qs(Req2),
  Opts = parse_opts(Qs),
  {RawPath, Req4} = cowboy_req:path(Req3),
  <<"/cdmi", Path/binary>> = RawPath,

  NewState = State#{cdmi_version => Version, options => Opts, path => Path},
  {false, Req4, NewState}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version from request arguments string.
%%--------------------------------------------------------------------
-spec get_supported_version(list() | binary()) ->
  binary() | undefined.
get_supported_version(undefined) -> undefined;
get_supported_version(VersionBinary) when is_binary(VersionBinary) ->
  VersionList = lists:map(fun utils:trim_spaces/1, binary:split(VersionBinary,<<",">>,[global])),
  get_supported_version(VersionList);
get_supported_version([]) -> throw(?unsupported_version);
get_supported_version([<<"1.1.1">> | _Rest]) -> <<"1.1.1">>;
get_supported_version([_Version | Rest]) -> get_supported_version(Rest).


%%--------------------------------------------------------------------
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling simple and range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc;ddd:fff") will return
%% [binary(aaa),{binary(bbb),1,2},binary(ccc),{binary(ddd),binary(fff)}]
%% @end
%%--------------------------------------------------------------------
-spec parse_opts(binary()) -> [binary() | {binary(), binary()} | {binary(), From :: integer(), To :: integer()}].
parse_opts(<<>>) ->
  [];
parse_opts(RawOpts) ->
  Opts = binary:split(RawOpts, <<";">>, [global]),
  lists:map(
    fun(Opt) ->
      case binary:split(Opt, <<":">>) of
        [SimpleOpt] -> SimpleOpt;
        [SimpleOpt, Range] ->
          case binary:split(Range, <<"-">>) of
            [SimpleVal] -> {SimpleOpt, SimpleVal};
            [From, To] ->
              {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
          end
      end
    end,
    Opts
  ).
