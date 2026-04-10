test_spi_resowner_sources = files(
  'test_spi_resowner.c',
)

if host_system == 'windows'
  test_spi_resowner_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_spi_resowner',
    '--FILEDESC', 'test_spi_resowner - SQL-callable C SPI function under a dedicated ResourceOwner',])
endif

test_spi_resowner = shared_module('test_spi_resowner',
  test_spi_resowner_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_spi_resowner

test_install_data += files(
  'test_spi_resowner.control',
  'test_spi_resowner--1.0.sql',
)

tests += {
  'name': 'test_spi_resowner',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'ri_fastpath',
    ],
  },
}
