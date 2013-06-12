%% This macro adds path to env_setter to code path so the test that uses env_setter should use this macro before first use of module
-define(INIT_DIST_TEST, code:add_path("../..")).