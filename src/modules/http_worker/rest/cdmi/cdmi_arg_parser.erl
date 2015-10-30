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

-export([get_supported_version/1, parse_opts/1, malformed_request/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version and options and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), dict()) -> {req(), dict()}.
malformed_request(Req, State) ->
  {RawVersion, Req2} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
  Version = get_supported_version(RawVersion),
  {Req3, Qs} = cowboy_req:qs(Req),
  Opts = parse_opts(Qs),
  State2 = dict:store(cdmi_version, Version, State),
  NewState = dict:store(options, Opts, State2),
  {Req3, NewState}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version from request arguments string.
%%--------------------------------------------------------------------
-spec get_supported_version(list() | binary()) ->
  binary() | {error, unsupported_version, binary()} | {error, unknown_version}.
get_supported_version(<<"">>) ->
  throw({error, unknown_version});
get_supported_version(BinaryString) when is_binary(BinaryString) ->
  BinaryList = [trim(X)  || X <- binary:split(BinaryString, <<",">>, [global])],
  get_supported_version(BinaryList);
get_supported_version([MaybeVersion | Tail]) ->
  case MaybeVersion of
    <<"1.1.1">> -> <<"1.1.1">>;
    _ -> case binary:split(MaybeVersion, <<".">>, [global]) of
      [X, Y, Z] when X =/= <<>>, Y =/= <<>>, Z =/= <<>> ->
        throw({error, unsupported_version, MaybeVersion});
      _ -> get_supported_version(Tail)
    end
  end;
get_supported_version([]) ->
  throw({error, unknown_version}).

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

%%--------------------------------------------------------------------
%% @doc Works like string:strip/1 but for binary().
%%--------------------------------------------------------------------
-spec trim(binary()) -> binary().
trim(Binary) ->
  list_to_binary(string:strip(binary_to_list(Binary))).