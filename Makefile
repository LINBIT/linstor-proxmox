DESTDIR=
PREFIX=/usr

export PERLDIR=${PREFIX}/share/perl5

all:
	@echo "The only useful target is 'deb'"

deb:
	dh_clean
	debuild -us -uc -i -b

install:
	install -D -m 0644 ./DRBDPlugin.pm.divert ${DESTDIR}$(PERLDIR)/PVE/Storage/DRBDPlugin.pm
	install -D -m 0644 ./DRBDPlugin.pm ${DESTDIR}$(PERLDIR)/PVE/Storage/Custom/DRBDPlugin.pm
