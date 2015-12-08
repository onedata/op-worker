%%%-------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides convinience functions designed for
%%% manipulating files' and containers' paths.
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_path).
-author("Piotr Ociepka").

%% API
-export([remove_trailing_slash/1, add_trailing_slash/1, ends_with_slash/1]).

-spec remove_trailing_slash(Path :: binary()) -> binary().
remove_trailing_slash(Path) ->
    binary:part(Path, {0, byte_size(Path)-1}).

-spec add_trailing_slash(Path :: binary()) -> binary().
add_trailing_slash(Path) ->
    <<Path/binary, $/>>.

%%--------------------------------------------------------------------
%% @equiv binary:last(Path) =:= $/
%%--------------------------------------------------------------------
-spec ends_with_slash(binary()) -> boolean().
ends_with_slash(Path) ->
    binary:last(Path) =:= $/.
