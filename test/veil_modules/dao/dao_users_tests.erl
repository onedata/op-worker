%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_users, using eunit tests.
%% @end
%% ===================================================================
-module(dao_users_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {foreach,
        fun() ->
            meck:new(dao),
            meck:new(lager)
        end,
        fun(_) ->
            ok = meck:unload(dao),
            ok = meck:unload(lager)
        end,
        [
            {"save user doc",
                fun() ->
                    meck:expect(dao, save_record, 
                        fun(Arg) -> 
                            case Arg of 
                                #veil_document{record = #user{}, uuid = ""} -> {ok, "new_uuid"};
                                #veil_document{record = #user{}, uuid = UUID} -> {ok, UUID}
                            end
                        end),
                    meck:expect(dao, set_db, fun(?USERS_DB_NAME) -> ok end),
                    ?assertEqual({ok, "new_uuid"}, dao_users:save_user(#user{})),
                    ?assertEqual({ok, "existing_uuid"}, dao_users:save_user(#veil_document{record = #user{}, uuid = "existing_uuid"})),
                    ?assert(meck:validate(dao))
                end},

            {"remove & get user doc by uuid",
                fun() ->
                    meck:expect(dao, set_db, fun(?USERS_DB_NAME) -> ok end),
                    meck:expect(dao, get_record, fun(_UUID) -> {ok, #veil_document{ record = #user{}, uuid = "existing_uuid" }} end),
                    meck:expect(dao, remove_record, fun(_UUID) -> ok end),
                    ?assertEqual({ok, #veil_document{ record = #user{}, uuid = "existing_uuid" }}, dao_users:get_user({uuid, "existing_uuid"})),
                    ?assertEqual(ok, dao_users:remove_user({uuid, "existing_uuid"})),
                    ?assert(meck:validate(dao))
                end},

            {"remove & get user doc from views",
                fun() ->
                    meck:expect(dao, set_db, fun(?USERS_DB_NAME) -> ok end),
                    meck:expect(dao, remove_record, fun(_UUID) -> ok end),
                    UserByLogin = #veil_document{ record = #user{ login = "login" } },
                    UserByEmail = #veil_document{ record = #user{ email_list = ["email"] } },
                    UserByDn = #veil_document{ record = #user{ dn_list = ["dn"] } },
                    LoginQueryArgs = #view_query_args{keys = [dao_helper:name("login")], include_docs = true},
                    EmailQueryArgs = #view_query_args{keys = [dao_helper:name("email")], include_docs = true},
                    DnQueryArgs = #view_query_args{keys = [dao_helper:name("dn")], include_docs = true},
                    meck:expect(dao, list_records, 
                        fun(View, QueryArgs) ->
                            case {View, QueryArgs} of
                                {?USER_BY_LOGIN_VIEW, LoginQueryArgs} -> {ok,  #view_result{rows = [#view_row{doc = UserByLogin}]}};
                                {?USER_BY_EMAIL_VIEW, EmailQueryArgs} -> {ok,  #view_result{rows = [#view_row{doc = UserByEmail}]}};
                                {?USER_BY_DN_VIEW, DnQueryArgs} -> {ok,  #view_result{rows = [#view_row{doc = UserByDn}]}}
                            end                                        
                        end),

                    ?assertEqual({ok, UserByLogin}, dao_users:get_user({login, "login"})),
                    ?assertEqual({ok, UserByEmail}, dao_users:get_user({email, "email"})),
                    ?assertEqual({ok, UserByDn}, dao_users:get_user({dn, "dn"})),
                    ?assertEqual(ok, dao_users:remove_user({login, "login"})),
                    ?assertEqual(ok, dao_users:remove_user({email, "email"})),
                    ?assertEqual(ok, dao_users:remove_user({dn, "dn"})),
                    ?assert(meck:validate(dao))
                end},

            {"empty, duplicated or invalid view response",
                fun() ->
                    meck:expect(dao, set_db, fun(?USERS_DB_NAME) -> ok end),
                    meck:expect(lager, log, fun(error, _, _, _) -> ok end),
                    meck:expect(lager, log, fun(warning, _, _, _) -> ok end),
                    UserDoc = #veil_document{ record = #user{ } },
                    QueryArgsEmptyResponse = #view_query_args{keys = [dao_helper:name("login")], include_docs = true},
                    QueryArgsDuplicatedResponse = #view_query_args{keys = [dao_helper:name("email")], include_docs = true},
                    QueryArgsInvalidResponse = #view_query_args{keys = [dao_helper:name("dn")], include_docs = true},
                    meck:expect(dao, list_records, 
                        fun(View, QueryArgs) ->
                            case {View, QueryArgs} of
                                {?USER_BY_LOGIN_VIEW, QueryArgsEmptyResponse} -> {ok,  #view_result{rows = []}};
                                {?USER_BY_EMAIL_VIEW, QueryArgsDuplicatedResponse} -> {ok,  #view_result{rows = [#view_row{doc = UserDoc}, #view_row{doc = UserDoc}]}};
                                {?USER_BY_DN_VIEW, QueryArgsInvalidResponse} -> {ok, ubelibubelimuk}
                            end                                        
                        end),

                    ?assertEqual(user_not_found, catch dao_users:get_user({login, "login"})),
                    ?assertEqual({ok, UserDoc}, dao_users:get_user({email, "email"})),
                    ?assertEqual(invalid_data, catch dao_users:get_user({dn, "dn"})),
                    ?assert(meck:validate(dao)),
                    ?assert(meck:validate(lager))
                end}
    ]}.

-endif.