%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides fslogic access control list utility
%% functions
%% @end
%% ===================================================================
-module(fslogic_acl).

-include("veil_modules/fslogic/fslogic_acl.hrl").
-include("fuse_messages_pb.hrl").

%% API
-export([ace_to_proplist/1, proplist_to_ace/1, acl_to_json_format/1, from_json_fromat_to_acl/1]).

%% acl_to_json_format/1
%% ====================================================================
%% @doc Parses list of access control entities to format suitable for mochijson2:encode
%% @end
-spec acl_to_json_format(Acl :: [#accesscontrolentity{}]) -> Result when
    Result :: list().
%% ====================================================================
acl_to_json_format(Acl) ->
    [ace_to_proplist(Ace) || Ace <- Acl].

%% from_json_fromat_to_acl/1
%% ====================================================================
%% @doc Parses proplist decoded json obtained from mochijson2:decode to
%% list of access control entities
%% @end
-spec from_json_fromat_to_acl(JsonAcl :: list()) -> [#accesscontrolentity{}].
%% ====================================================================
from_json_fromat_to_acl(JsonAcl) ->
    [proplist_to_ace(AceProplist) || AceProplist <- JsonAcl].


%% ace_to_proplist/1
%% ====================================================================
%% @doc Parses access control entity to format suitable for mochijson2:encode
%% @end
-spec ace_to_proplist(#accesscontrolentity{}) -> list().
%% ====================================================================
ace_to_proplist(#accesscontrolentity{acetype = Type, aceflags = Flags, identifier = Who, acemask = AccessMask}) ->
    [
        {<<"acetype">>, bitmask_to_type(Type)},
        {<<"identifier">>, Who},
        {<<"aceflags">>, binary_list_to_csv(bitmask_to_flag_list(Flags))},
        {<<"acemask">>, binary_list_to_csv(bitmask_to_perm_list(AccessMask))}
    ].


%% proplist_to_ace/1
%% ====================================================================
%% @doc Parses proplist decoded json obtained from mochijson2:decode to
%% access control entity
%% @end
-spec proplist_to_ace(List :: list()) -> #accesscontrolentity{}.
%% ====================================================================
proplist_to_ace(List) -> proplist_to_ace2(List,#accesscontrolentity{acetype = undefined, aceflags = undefined, identifier = undefined, acemask = undefined}).
proplist_to_ace2([], Acc) -> Acc;
proplist_to_ace2([{<<"acetype">>, Type} | Rest], Acc) -> proplist_to_ace2(Rest, Acc#accesscontrolentity{acetype = type_to_bitmask(Type)});
proplist_to_ace2([{<<"identifier">>, Identifier} | Rest], Acc) -> proplist_to_ace2(Rest, Acc#accesscontrolentity{identifier = Identifier});
proplist_to_ace2([{<<"aceflags">>, Flags} | Rest], Acc) -> proplist_to_ace2(Rest, Acc#accesscontrolentity{aceflags = flags_to_bitmask(Flags)});
proplist_to_ace2([{<<"acemask">>, AceMask} | Rest], Acc) -> proplist_to_ace2(Rest, Acc#accesscontrolentity{acemask = acemask_to_bitmask(AceMask)}).

%% bitmask_to_type/1
%% ====================================================================
%% @doc maps bitmask to acetype
%% @end
-spec bitmask_to_type(non_neg_integer()) -> binary().
%% ====================================================================
bitmask_to_type(?allow_mask) -> ?allow;
bitmask_to_type(?deny_mask) -> ?deny;
bitmask_to_type(?audit_mask) -> ?audit;
bitmask_to_type(_) -> undefined.

%% bitmask_to_flag_list/1
%% ====================================================================
%% @doc maps bitmask to aceflags
%% @end
-spec bitmask_to_flag_list(non_neg_integer()) -> [binary()].
%% ====================================================================
bitmask_to_flag_list(Hex) -> lists:reverse(bitmask_to_flag_list2(Hex, [])).
bitmask_to_flag_list2(Hex, List) when (Hex band ?identifier_group_mask) == ?identifier_group_mask  ->
    bitmask_to_flag_list2(Hex xor ?identifier_group_mask, [?identifier_group | List]);
bitmask_to_flag_list2(?no_flags_mask, []) -> [?no_flags];
bitmask_to_flag_list2(?no_flags_mask, List) -> List;
bitmask_to_flag_list2(_, _) -> undefined.

%% bitmask_to_perm_list/1
%% ====================================================================
%% @doc maps bitmask to perm list
%% @end
-spec bitmask_to_perm_list(non_neg_integer()) -> [binary()].
%% ====================================================================
bitmask_to_perm_list(Hex) -> lists:reverse(bitmask_to_perm_list2(Hex, [])).
bitmask_to_perm_list2(Hex, List) when (Hex band ?read_mask) == ?read_mask  -> bitmask_to_flag_list2(Hex xor ?read_mask, [?read | List]);
bitmask_to_perm_list2(Hex, List) when (Hex band ?write_mask) == ?write_mask  -> bitmask_to_flag_list2(Hex xor ?write_mask, [?write | List]);
bitmask_to_perm_list2(16#00000000, List) -> List;
bitmask_to_perm_list2(_, _) -> undefined.

%% type_to_bitmask/1
%% ====================================================================
%% @doc map acetype to bitmask
%% @end
-spec type_to_bitmask(binary()) -> non_neg_integer().
%% ====================================================================
type_to_bitmask(?allow) -> ?allow_mask;
type_to_bitmask(?deny) -> ?deny_mask;
type_to_bitmask(?audit) -> ?audit_mask.

%% flags_to_bitmask/1
%% ====================================================================
%% @doc maps coma separated binary of aceflags to bitmask
%% @end
-spec flags_to_bitmask(binary()) -> non_neg_integer().
%% ====================================================================
flags_to_bitmask(Flags) when is_binary(Flags) ->
    flags_to_bitmask(csv_to_binary_list(Flags));
flags_to_bitmask([]) -> ?no_flags_mask;
flags_to_bitmask([?no_flags | Rest]) -> ?no_flags_mask bor flags_to_bitmask(Rest);
flags_to_bitmask([?identifier_group | Rest]) -> ?identifier_group_mask bor flags_to_bitmask(Rest).


%% acemask_to_bitmask/1
%% ====================================================================
%% @doc maps coma separated binary of permissions to bitmask
%% @end
-spec acemask_to_bitmask(binary()) -> non_neg_integer().
%% ====================================================================
acemask_to_bitmask(MaskNames) when is_binary(MaskNames) ->
    FlagList = lists:map(fun fslogic_utils:trim_spaces/1, binary:split(MaskNames, <<",">>, [global])),
    acemask_to_bitmask(FlagList);
acemask_to_bitmask([]) -> 16#00000000;
acemask_to_bitmask([?read | Rest]) -> ?read_mask bor acemask_to_bitmask(Rest);
acemask_to_bitmask([?write | Rest]) -> ?write_mask bor acemask_to_bitmask(Rest).

%% binary_list_to_csv/1
%% ====================================================================
%% @doc converts list of binaries to one, coma separated binary.
%% i. e. binary_list_to_csv(<<"a">>, <<"b">>) -> <<"a, b">>
%% @end
-spec binary_list_to_csv([binary()]) -> binary().
%% ====================================================================
binary_list_to_csv(List) ->
    lists:foldl(
        fun(Name, <<"">>) -> Name;
            (Name, Acc) -> <<Acc/binary, ", ", Name/binary>>
        end, <<"">>, List).

%% csv_to_binary_list/1
%% ====================================================================
%% @doc converts coma separated binary to list of binaries,
%% i. e. binary_list_to_csv(<<"a, b">>) -> [<<"a">>, <<"b">>]
%% @end
-spec csv_to_binary_list([binary()]) -> binary().
%% ====================================================================
csv_to_binary_list(BinaryCsv) ->
    lists:map(fun fslogic_utils:trim_spaces/1, binary:split(BinaryCsv, <<",">>, [global])).
