%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jul 2014 11:15
%%%-------------------------------------------------------------------
-author("RoXeon").

-ifndef(DAO_SPACES_HRL).
-define(DAO_SPACES_HRL, 1).

-define(file_space_info_extestion, space_info).
-record(space_info, {uuid = "", name = "", providers = []}).

-endif.