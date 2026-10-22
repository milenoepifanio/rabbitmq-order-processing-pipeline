# Security Policy

## Project status

This is an educational project, not a production service. No production deployments or security support commitments are provided.

## Reporting a vulnerability

Please **do not publish security vulnerabilities, credentials, or sensitive data in public issues**.

If GitHub private vulnerability reporting is enabled for this repository, use **Security → Report a vulnerability**. Otherwise, contact the repository maintainer privately through a contact channel they have explicitly published. Do not assume that private reporting is enabled.

Provide a concise description, reproduction steps, affected files, and potential impact. Avoid including real credentials or personal information.

## Security considerations

- The default RabbitMQ `guest` account is suitable only for isolated local learning; use dedicated credentials in other environments.
- Do not commit `.env` files, tokens, passwords, or secrets.
- Avoid exposing RabbitMQ ports to untrusted networks.
- Keep Docker images and Python dependencies reasonably up to date.
- Use fictional data for all examples.

## Supported versions

There is no formal supported-version policy for this learning repository.
